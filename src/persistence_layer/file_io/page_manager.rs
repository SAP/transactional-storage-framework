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

    /// Creates a new page.
    ///
    /// This tries to search for a free page in the corresponding segment of the supplied address,
    /// and then search the associated segment directory, and then the entire database file.
    #[allow(dead_code, clippy::unused_async)]
    pub async fn create_page<R, F: FnOnce(u64, &mut EvictablePage) -> R>(
        &self,
        known_address: Address,
        _writer: F,
    ) -> Result<R, Error> {
        // TODO: check out the free page directory, and send a request to the IO task processor to
        // get a new page if none is free.
        let segment_address = known_address.segment_address();
        let free_page_in_segment = self
            .write_page(segment_address, |page| {
                if page.is_first_bit_set() {
                    // The segment was deleted.
                    return 0;
                }
                for (offset, d) in page.buffer_mut().iter_mut().enumerate() {
                    let free_index = d.trailing_ones();
                    if free_index < u8::BITS {
                        *d |= 1_u8 << free_index;
                        page.set_dirty();
                        return offset * (u8::BITS as usize) + free_index as usize;
                    }
                }
                0
            })
            .await?;
        if free_page_in_segment == 0 {
            // Search the segment directory.
            Err(Error::UnexpectedState)
        } else {
            todo!()
        }
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
                let (evicted, mut inserted) = v.put_entry(Box::new(evictable_page));
                if let Some((_, mut evicted)) = evicted {
                    if let Err(e) = evicted.write_back(&self.db) {
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
        page_address: Address,
        writer: F,
    ) -> Result<R, Error> {
        debug_assert_eq!(page_address, page_address.page_address());
        let (evicted, result) = match self.page_cache.entry_async(page_address).await {
            Entry::Occupied(mut o) => return Ok(writer(o.get_mut())),
            Entry::Vacant(v) => {
                let evictable_page = EvictablePage::from_file(&self.db, page_address.into())?;
                let (evicted, mut inserted) = v.put_entry(Box::new(evictable_page));
                (evicted, writer(inserted.get_mut()))
            }
        };
        if let Some((_, evicted)) = evicted {
            if evicted.is_dirty() {
                drop(
                    self.file_io_task_sender
                        .send(IOTask::WriteBackEvicted(evicted)),
                );
            }
        }
        Ok(result)
    }

    /// Writes back a dirty page.
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
