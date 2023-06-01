// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

//! Page management.

use super::addressing::Address;
use super::database_header::DatabaseHeader;
use super::evictable_page::EvictablePage;
use super::io_task_processor::IOTask;
use super::RandomAccessFile;
use crate::Error;
use scc::hash_cache::Entry;
use scc::HashCache;
use std::sync::mpsc::SyncSender;
use std::thread::yield_now;

/// [`PageManager`] provides an interface between the database workers and the persistence layer to
/// make use of persistent pages.
#[derive(Debug)]
pub struct PageManager {
    /// The database file.
    db: RandomAccessFile,

    /// The database header.
    #[allow(dead_code)]
    db_header: DatabaseHeader,

    /// Cached pages.
    page_cache: HashCache<Address, Box<EvictablePage>>,

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
    #[allow(dead_code, clippy::unused_async)]
    pub async fn create_page<R, F: FnOnce(u64, &mut EvictablePage) -> R>(
        &self,
        _previous_page: Address,
        _writer: F,
    ) -> Result<R, Error> {
        // TODO: check out the free page directory, and send a request to the IO task processor to
        // get a new page if none is free.
        Err(Error::UnexpectedState)
    }

    /// Deletes an existing page.
    ///
    /// Returns the new size of the file.
    #[allow(dead_code, clippy::unused_async)]
    pub async fn delete_page(&self, _page_address: Address) -> Result<u64, Error> {
        // TODO: push the page into the free page directory or truncate the file.
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
        page_address: Address,
        reader: F,
    ) -> Result<R, Error> {
        debug_assert_eq!(page_address, page_address.page_address());
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
                let evictable_page = EvictablePage::from_file(&self.db, page_address.into())?;
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
        page_address: Address,
        writer: F,
    ) -> Result<R, Error> {
        debug_assert_eq!(page_address, page_address.page_address());
        match self.page_cache.entry_async(page_address).await {
            Entry::Occupied(mut o) => Ok(writer(o.get_mut())),
            Entry::Vacant(v) => {
                let evictable_page = EvictablePage::from_file(&self.db, page_address.into())?;
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
    pub fn write_back_sync(&self, page_address: Address) {
        debug_assert_eq!(page_address, page_address.page_address());
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
}

impl Drop for PageManager {
    #[inline]
    fn drop(&mut self) {
        // TODO: cleanup pages.
    }
}
