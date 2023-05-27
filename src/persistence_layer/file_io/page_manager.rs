// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

//! Page management.

#![allow(dead_code)]

use super::database_header::DatabaseHeader;
use super::evictable_page::EvictablePage;
use super::io_task_processor::IOTask;
use super::segment::PAGE_SIZE;
use super::RandomAccessFile;
use crate::Error;
use scc::hash_cache::Entry;
use scc::HashCache;
use std::sync::mpsc::SyncSender;

/// [`PageManager`] provides an interface between the database workers and the persistence layer to
/// make use of persistent pages.
#[derive(Debug)]
pub struct PageManager {
    /// The database file.
    db: RandomAccessFile,

    /// The database header.
    db_header: DatabaseHeader,

    /// Cached pages.
    page_cache: HashCache<u64, EvictablePage>,

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

    /// Creates a new page.
    #[allow(clippy::unused_async)]
    pub async fn create_page<R, F: FnOnce(u64, &mut EvictablePage) -> R>(
        &self,
        _writer: F,
    ) -> Result<R, Error> {
        // TODO: check out the free page directory, and send a request to the IO task processor to
        // get a new page if none is free.
        Err(Error::UnexpectedState)
    }

    /// Deletes an existing page.
    ///
    /// Returns the new size of the file.
    #[allow(clippy::unused_async)]
    pub async fn delete_page(&self, _offset: u64) -> Result<u64, Error> {
        // TODO: push the page into the free page directory or truncate the file.
        Err(Error::UnexpectedState)
    }

    /// Reads a page in the database.
    ///
    /// # Errors
    ///
    /// Returns an error if the page could not be read.
    #[inline]
    pub async fn read_page<R, F: FnOnce(&EvictablePage) -> R>(
        &self,
        offset: u64,
        reader: F,
    ) -> Result<R, Error> {
        debug_assert_eq!(offset % PAGE_SIZE, 0);
        let mut reader = Some(reader);
        if let Some(result) = self
            .page_cache
            .read_async(&offset, |_, v| reader.take().unwrap()(v))
            .await
        {
            return Ok(result);
        }

        match self.page_cache.entry_async(offset).await {
            Entry::Occupied(o) => Ok(reader.unwrap()(o.get())),
            Entry::Vacant(v) => {
                let evictable_page = EvictablePage::from_file(&self.db, offset)?;
                let (evicted, mut inserted) = v.put_entry(evictable_page);
                if let Some((_, mut evicted)) = evicted {
                    if let Err(e) = evicted.write_back(&self.db, offset) {
                        // Do not evict the entry.
                        inserted.put(evicted);
                        return Err(e);
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
    #[inline]
    pub async fn write_page<R, F: FnOnce(&mut EvictablePage) -> R>(
        &self,
        offset: u64,
        writer: F,
    ) -> Result<R, Error> {
        debug_assert_eq!(offset % PAGE_SIZE, 0);
        match self.page_cache.entry_async(offset).await {
            Entry::Occupied(mut o) => Ok(writer(o.get_mut())),
            Entry::Vacant(v) => {
                let evictable_page = EvictablePage::from_file(&self.db, offset)?;
                let (evicted, mut inserted) = v.put_entry(evictable_page);
                if let Some((_, mut evicted)) = evicted {
                    if let Err(e) = evicted.write_back(&self.db, offset) {
                        // Do not evict the entry.
                        inserted.put(evicted);
                        return Err(e);
                    }
                }
                Ok(writer(inserted.get_mut()))
            }
        }
    }
}

impl Drop for PageManager {
    #[inline]
    fn drop(&mut self) {
        // TODO: cleanup pages.
    }
}
