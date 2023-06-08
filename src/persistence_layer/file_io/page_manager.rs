// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

//! Page management.

use super::database_header::DatabaseHeader;
use super::evictable_page::{EvictablePage, PAGE_SIZE};
use super::io_task_processor::IOTask;
use super::RandomAccessFile;
use crate::Error;
use scc::hash_cache::Entry;
use scc::{Bag, HashCache};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::mpsc::SyncSender;
use std::task::{Context, Poll, Waker};
use std::thread::yield_now;

/// [`PageManager`] provides an interface between the database workers and the persistence layer to
/// make use of persistent pages.
#[derive(Debug)]
pub struct PageManager {
    /// The database file.
    db: RandomAccessFile,

    /// The database header.
    db_header: DatabaseHeader,

    /// Cached pages.
    page_cache: HashCache<u64, Box<EvictablePage>>,

    /// File IO task sender.
    file_io_task_sender: SyncSender<IOTask>,

    /// [`Waker`] bag for free pages.
    waker_bag_for_free_page: Bag<Waker>,

    /// [`Waker`] bag for caching pages.
    waker_bag_for_caching_page: Bag<Waker>,
}

/// Free page allocation unit.
pub const ALLOCATION_UNIT: u64 = PAGE_SIZE * 2048;

/// [`AwaitFreePage`] waits until a free page is available for the corresponding page manager.
#[derive(Debug)]
pub struct AwaitFreePage<'p> {
    /// The page manager to provide free pages for which the [`AwaitFreePage`] waits.
    page_manager: &'p PageManager,
}

/// [`AwaitCachedPage`] waits until the specified page is cached from the file.
#[derive(Debug)]
pub struct AwaitCachedPage<'p> {
    /// The page manager to provide free pages for which the [`AwaitCachedPage`] waits.
    page_manager: &'p PageManager,

    /// The page address.
    page_address: u64,
}

impl PageManager {
    /// Creates a new [`PageManager`].
    #[inline]
    pub fn from_db(
        db: RandomAccessFile,
        file_io_task_sender: SyncSender<IOTask>,
    ) -> Result<Self, Error> {
        let db_header = DatabaseHeader::from_file(&db)?;
        Ok(Self {
            db,
            db_header,
            page_cache: HashCache::with_capacity(0x10, 0x100_0000),
            file_io_task_sender,
            waker_bag_for_free_page: Bag::default(),
            waker_bag_for_caching_page: Bag::default(),
        })
    }

    /// Creates a new page and appends the newly created page to the specified page.
    ///
    /// This assumes that the caller owns the page chain.
    #[allow(dead_code)]
    #[inline]
    pub async fn create_page(&self, prev_page_address: u64) -> Result<u64, Error> {
        debug_assert_eq!(prev_page_address % PAGE_SIZE, 0);
        loop {
            let free_page_address = self.get_free_page().await?;
            let result = self
                .write_page(free_page_address, |free_page| {
                    if free_page.prev_page_address() != 0 {
                        // The free page is linked to another page.
                        return false;
                    }
                    // The free page is no longer free.
                    free_page.set_prev_page_address(prev_page_address);
                    true
                })
                .await?;
            if result {
                // Need to write back the data before writing anything to the free page.
                drop(
                    self.file_io_task_sender
                        .send(IOTask::WriteBack(free_page_address)),
                );
                return Ok(free_page_address);
            }
        }
    }

    /// Deletes an existing page.
    ///
    /// This assumes that the caller owns the page chain.
    #[allow(dead_code)]
    #[inline]
    pub async fn delete_page(&self, page_address: u64) -> Result<(), Error> {
        debug_assert_eq!(page_address % PAGE_SIZE, 0);
        let (prev_page_address, next_page_address) = self
            .read_page(page_address, |e| {
                (e.prev_page_address(), e.next_page_address())
            })
            .await?;

        // 1. Update `prev.next`, and write back.
        if prev_page_address != 0 {
            self.write_page(prev_page_address, |e| {
                e.set_next_page_address(next_page_address);
                e.set_dirty();
            })
            .await?;
            drop(
                self.file_io_task_sender
                    .send(IOTask::WriteBack(prev_page_address)),
            );
        }

        // 2. Update `next.prev`, and write back.
        if next_page_address != 0 {
            self.write_page(next_page_address, |e| {
                e.set_prev_page_address(prev_page_address);
                e.set_dirty();
            })
            .await?;
            drop(
                self.file_io_task_sender
                    .send(IOTask::WriteBack(next_page_address)),
            );
        }

        // 3. Update `free.prev`.
        self.write_page(page_address, |e| {
            e.set_prev_page_address(0);
            e.set_next_page_address(0);
            e.set_dirty();
        })
        .await?;

        // Two cases to handle during recovery.
        // 1. `1 -> 2 -> Crash`.
        // - Backtrack from `next`; if reachable immediately, `3` shall be performed separately by
        //  a free page scanner.
        // 3. `1 -> Crash`.
        // - Backtrack from `next`; if reachable in two steps, proceed with `2` and `3`.

        self.add_free_page(page_address);

        Ok(())
    }

    /// Reads a page in the database.
    ///
    /// # Errors
    ///
    /// Returns an error if the page could not be read.
    #[inline]
    pub async fn read_page<R, F: FnOnce(&EvictablePage) -> R>(
        &self,
        page_address: u64,
        reader: F,
    ) -> Result<R, Error> {
        debug_assert_eq!(page_address % PAGE_SIZE, 0);
        let mut reader = Some(reader);
        loop {
            if let Some(result) = self
                .page_cache
                .read_async(&page_address, |_, v| reader.take().unwrap()(v))
                .await
            {
                return Ok(result);
            }
            drop(
                self.file_io_task_sender
                    .send(IOTask::FillCache(page_address)),
            );
            AwaitCachedPage {
                page_manager: self,
                page_address,
            }
            .await;
        }
    }

    /// Writes a page in the database.
    ///
    /// # Errors
    ///
    /// Returns an error if the page could not be modified.
    #[inline]
    pub async fn write_page<R, F: FnOnce(&mut EvictablePage) -> R>(
        &self,
        page_address: u64,
        writer: F,
    ) -> Result<R, Error> {
        debug_assert_eq!(page_address % PAGE_SIZE, 0);
        loop {
            if let Entry::Occupied(mut o) = self.page_cache.entry_async(page_address).await {
                return Ok(writer(o.get_mut()));
            }
            drop(
                self.file_io_task_sender
                    .send(IOTask::FillCache(page_address)),
            );
            AwaitCachedPage {
                page_manager: self,
                page_address,
            }
            .await;
        }
    }

    /// Fills the cache entry corresponding to the specified page address.
    pub(super) fn fill_cache_sync(&self, page_address: u64) {
        debug_assert_eq!(page_address % PAGE_SIZE, 0);
        while let Entry::Vacant(v) = self.page_cache.entry(page_address) {
            let Ok(evictable_page) = EvictablePage::from_file(&self.db, page_address) else {
                drop(v);
                yield_now();
                continue;
            };
            let (evicted, inserted) = v.put_entry(Box::new(evictable_page));
            if let Some((_, mut evicted)) = evicted {
                if evicted.is_dirty() {
                    drop(inserted);

                    // It is OK to process it directly outside the critical section since a newer
                    // version of the page, if there is any, shall be written back after it.
                    self.write_back_evicted_sync(&mut evicted);
                }
            }
            break;
        }
        self.waker_bag_for_caching_page.pop_all((), |_, w| w.wake());
    }

    /// Resizes the database file.
    ///
    /// It is a synchronous method, therefore it should be run in the background.
    pub(super) fn resize_sync(&self, new_size: u64) {
        debug_assert_eq!(new_size % PAGE_SIZE, 0);
        let old_size = self.db.len(Relaxed);
        debug_assert_eq!(old_size % PAGE_SIZE, 0);
        while self.db.set_len(new_size).is_err() {
            yield_now();
        }
        for i in old_size / PAGE_SIZE..new_size / PAGE_SIZE {
            let free_page_address = i * PAGE_SIZE;
            self.add_free_page(free_page_address);
        }
    }

    /// Writes back a dirty page with the page retained in the cache.
    ///
    /// It is a synchronous method, therefore it should be run in the background.
    pub(super) fn write_back_sync(&self, page_address: u64) {
        debug_assert_eq!(page_address % PAGE_SIZE, 0);
        while let Some(mut o) = self.page_cache.get(&page_address) {
            if o.get_mut().write_back(&self.db).is_ok() {
                break;
            }
            drop(o);
            yield_now();
        }
    }

    /// Write back the evicted page.
    ///
    /// It is a synchronous method, therefore it should be run in the background.
    pub(super) fn write_back_evicted_sync(&self, page: &mut EvictablePage) {
        while page.write_back(&self.db).is_err() {
            yield_now();
        }
    }

    /// Gets a free page.
    async fn get_free_page(&self) -> Result<u64, Error> {
        loop {
            if let Some(free_page_address) = self.db_header.free_pages.pop() {
                return Ok(free_page_address);
            }

            // It is mandated to read at least `2048` pages.
            let file_len = self.db.len(Relaxed);
            let num_pages_to_read = ALLOCATION_UNIT / PAGE_SIZE;
            let mut resize_file = false;
            let read_from =
                self.db_header
                    .free_page_scanner_offset
                    .fetch_update(Relaxed, Relaxed, |o| {
                        if let Some(new_offset) = o.checked_add(ALLOCATION_UNIT) {
                            if new_offset >= file_len {
                                resize_file = true;
                            } else {
                                return Some(new_offset);
                            }
                        }

                        // Integer overflow.
                        None
                    });
            if let Ok(read_from) = read_from {
                for i in 0..num_pages_to_read {
                    #[allow(clippy::no_effect_underscore_binding)]
                    let _offset = read_from + PAGE_SIZE * i;
                    // TODO: asynchronously read the header.
                }
            } else if resize_file {
                drop(
                    self.file_io_task_sender
                        .send(IOTask::Resize(file_len + ALLOCATION_UNIT)),
                );
                AwaitFreePage { page_manager: self }.await;
            } else {
                // TODO: handle the integer overflow.
                todo!();
            }
        }
    }

    /// Adds a free page.
    fn add_free_page(&self, free_page_address: u64) {
        debug_assert_eq!(free_page_address % PAGE_SIZE, 0);
        self.db_header.free_pages.push(free_page_address);
        self.waker_bag_for_free_page.pop_all((), |_, w| w.wake());
    }
}

impl<'p> Future for AwaitFreePage<'p> {
    type Output = ();

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.page_manager.db_header.free_pages.is_empty() {
            return Poll::Ready(());
        }

        self.page_manager
            .waker_bag_for_free_page
            .push(cx.waker().clone());
        if self.page_manager.db_header.free_pages.is_empty() {
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }
}

impl<'p> Future for AwaitCachedPage<'p> {
    type Output = ();

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // TODO: it is a synchronous call, fix me later.
        if self.page_manager.page_cache.contains(&self.page_address) {
            return Poll::Ready(());
        }
        self.page_manager
            .waker_bag_for_caching_page
            .push(cx.waker().clone());
        if self.page_manager.page_cache.contains(&self.page_address) {
            return Poll::Ready(());
        }
        Poll::Pending
    }
}

#[cfg(test)]
mod test {
    use super::PAGE_SIZE;
    use crate::{FileIO, MonotonicU64};
    use std::path::Path;
    use tokio::fs::remove_dir_all;

    #[tokio::test]
    async fn create() {
        const DIR: &str = "page_manager_create_test";
        let path = Path::new(DIR);

        let data: u8 = 17;

        let file_io = FileIO::<MonotonicU64>::with_path(path).unwrap();
        let mut free_pages = Vec::new();
        for i in 0..4 {
            let free_page = file_io.page_manager().create_page(PAGE_SIZE).await.unwrap();
            assert_ne!(free_page, 0);
            assert!(file_io
                .page_manager()
                .write_page(free_page, |e| e.buffer_mut()[0] = data + i)
                .await
                .is_ok());
            file_io.page_manager().write_back_sync(free_page);
            free_pages.push(free_page);
        }
        drop(file_io);

        let file_io_recovered = FileIO::<MonotonicU64>::with_path(path).unwrap();
        for (offset, free_page) in free_pages.iter().enumerate() {
            let result = file_io_recovered
                .page_manager()
                .read_page(*free_page, |e| e.buffer()[0])
                .await
                .unwrap();
            {
                #![allow(clippy::cast_possible_truncation)]
                assert_eq!(result, data + offset as u8);
            }
        }

        drop(file_io_recovered);

        assert!(remove_dir_all(path).await.is_ok());
    }

    #[tokio::test]
    async fn delete() {
        const DIR: &str = "page_manager_delete_test";
        let path = Path::new(DIR);

        let data: u8 = 19;

        let file_io = FileIO::<MonotonicU64>::with_path(path).unwrap();
        let free_page = file_io.page_manager().create_page(PAGE_SIZE).await.unwrap();
        assert_ne!(free_page, 0);
        assert!(file_io
            .page_manager()
            .write_page(free_page, |e| e.buffer_mut()[0] = data)
            .await
            .is_ok());
        file_io.page_manager().write_back_sync(free_page);
        assert!(file_io.page_manager().delete_page(free_page).await.is_ok());
        let free_page_again = file_io.page_manager().create_page(PAGE_SIZE).await.unwrap();
        assert_eq!(free_page_again, free_page);
        drop(file_io);

        let file_io_recovered = FileIO::<MonotonicU64>::with_path(path).unwrap();
        let result = file_io_recovered
            .page_manager()
            .read_page(free_page, |e| e.buffer()[0])
            .await
            .unwrap();
        assert_eq!(result, data);
        drop(file_io_recovered);

        assert!(remove_dir_all(path).await.is_ok());
    }
}
