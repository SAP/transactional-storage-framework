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
use scc::HashCache;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::mpsc::SyncSender;
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
        })
    }

    /// Creates a new page and appends the newly created page to the specified page.
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
    #[allow(dead_code, clippy::unused_async)]
    pub async fn delete_page(&self, page_address: u64) -> Result<(), Error> {
        debug_assert_eq!(page_address % PAGE_SIZE, 0);
        let (_prev_page_address, _next_page_address) = self
            .write_page(page_address, |e| {
                let prev_page_address = e.prev_page_address();
                e.set_prev_page_address(0);
                e.set_dirty();
                (prev_page_address, e.next_page_address())
            })
            .await?;
        // TODO: set the `NEXT_OFFSET` field of the previous page and set the `PREV_OFFSET` field
        // of the next page.
        self.add_free_page(page_address).await?;

        Err(Error::UnexpectedState)
    }

    /// Reads a page in the database.
    ///
    /// # Errors
    ///
    /// Returns an error if the page could not be read.
    #[allow(dead_code)]
    #[inline]
    pub async fn read_page<R, F: FnOnce(&EvictablePage) -> R>(
        &self,
        page_address: u64,
        reader: F,
    ) -> Result<R, Error> {
        debug_assert_eq!(page_address % PAGE_SIZE, 0);
        let mut reader = Some(reader);
        if let Some(result) = self
            .page_cache
            .read_async(&page_address, |_, v| reader.take().unwrap()(v))
            .await
        {
            return Ok(result);
        }

        match self.page_cache.entry_async(page_address).await {
            Entry::Occupied(o) => Ok(reader.unwrap()(o.get())),
            Entry::Vacant(v) => {
                // TODO: it is synchronous and blocking, make it asynchronous.
                let evictable_page = EvictablePage::from_file(&self.db, page_address)?;
                let (evicted, inserted) = v.put_entry(Box::new(evictable_page));
                if let Some((_, evicted)) = evicted {
                    if evicted.is_dirty() {
                        drop(
                            self.file_io_task_sender
                                .send(IOTask::WriteBackEvicted(evicted)),
                        );
                    }
                }
                Ok(reader.unwrap()(inserted.get()))
            }
        }
    }

    /// Writes a page in the database.
    ///
    /// # Errors
    ///
    /// Returns an error if the page could not be modified.
    #[allow(dead_code)]
    #[inline]
    pub async fn write_page<R, F: FnOnce(&mut EvictablePage) -> R>(
        &self,
        page_address: u64,
        writer: F,
    ) -> Result<R, Error> {
        debug_assert_eq!(page_address % PAGE_SIZE, 0);
        match self.page_cache.entry_async(page_address).await {
            Entry::Occupied(mut o) => Ok(writer(o.get_mut())),
            Entry::Vacant(v) => {
                let evictable_page = EvictablePage::from_file(&self.db, page_address)?;
                let (evicted, mut inserted) = v.put_entry(Box::new(evictable_page));
                if let Some((_, evicted)) = evicted {
                    if evicted.is_dirty() {
                        drop(
                            self.file_io_task_sender
                                .send(IOTask::WriteBackEvicted(evicted)),
                        );
                    }
                }
                Ok(writer(inserted.get_mut()))
            }
        }
    }

    /// Writes back a dirty page with the page retained in the cache.
    ///
    /// It is a synchronous method, therefore it should be run in the background.
    #[inline]
    pub fn write_back_sync(&self, page_address: u64) {
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
    #[inline]
    pub fn write_back_evicted_sync(&self, page: &mut EvictablePage) {
        while page.write_back(&self.db).is_err() {
            yield_now();
        }
    }

    /// Gets a free page.
    #[allow(clippy::unused_async)]
    #[inline]
    async fn get_free_page(&self) -> Result<u64, Error> {
        loop {
            if let Some(free_page_address) = self.db_header.free_pages.pop() {
                return Ok(free_page_address);
            }

            // It is mandated to read at least `2048` pages.
            let file_len = self.db.len(Relaxed);
            let num_pages_to_read = 2048;
            let mut resize_file = false;
            let read_from =
                self.db_header
                    .free_page_scanner_offset
                    .fetch_update(Relaxed, Relaxed, |o| {
                        if let Some(new_offset) = o.checked_add(PAGE_SIZE * num_pages_to_read) {
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
                    let _offset = read_from + PAGE_SIZE * i;
                    // TODO: asynchronously read the header.
                }
            } else if resize_file {
                // TODO: asynchronously resize the file.
                todo!();
            } else {
                // TODO: handle the integer overflow.
                todo!();
            }
        }
    }

    /// Adds a free page.
    #[allow(clippy::unused_async)]
    #[inline]
    async fn add_free_page(&self, _free_page_address: u64) -> Result<(), Error> {
        todo!()
    }
}

impl Drop for PageManager {
    #[inline]
    fn drop(&mut self) {
        // TODO: cleanup pages.
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
        let free_page = file_io.page_manager().create_page(PAGE_SIZE).await.unwrap();
        assert_ne!(free_page, 0);
        assert!(file_io
            .page_manager()
            .write_page(free_page, |e| e.buffer_mut()[0] = data)
            .await
            .is_ok());
        file_io.page_manager().write_back_sync(free_page);
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
