// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

//! Page management.

#![allow(dead_code)]

use super::db_header::DatabaseHeader;
use super::evictable_page::EvictablePage;
use super::page_io_task_processor;
use super::page_io_task_processor::PageIOTask;
use super::RandomAccessFile;
use crate::{utils, Error};
use scc::HashCache;
use std::sync::mpsc::{self, SyncSender, TrySendError};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

/// [`PageManager`] provides an interface between the database workers and the persistence layer to
/// make use of persistent pages.
#[derive(Debug)]
pub struct PageManager {
    /// The page IO worker thread.
    page_io_worker: Option<JoinHandle<()>>,

    /// The page IO task sender.
    page_io_task_sender: SyncSender<PageIOTask>,

    /// Shared data between [`PageManager`] and the page IO worker thread.
    page_manager_data: Arc<PageManagerData>,
}

/// Shared data between [`PageManager`] and the page IO worker thread.
#[allow(clippy::module_name_repetitions)]
#[derive(Debug)]
pub struct PageManagerData {
    /// The database file.
    db: RandomAccessFile,

    /// The database header.
    db_header: DatabaseHeader,

    /// Cached pages.
    page_cache: HashCache<u64, EvictablePage>,
}

impl PageManager {
    /// Creates a new [`PageManager`].
    #[inline]
    pub fn from_db(db: RandomAccessFile) -> Result<Self, Error> {
        let db_header = DatabaseHeader::from_file(&db)?;
        let (page_io_task_sender, mut page_io_task_receiver) =
            mpsc::sync_channel::<PageIOTask>(utils::advise_num_shards() * 4);
        let page_manager_data = Arc::new(PageManagerData {
            db,
            db_header,
            page_cache: HashCache::with_capacity(0x10, 0x100_0000),
        });
        let page_manager_data_clone = page_manager_data.clone();
        Ok(Self {
            page_io_worker: Some(thread::spawn(move || {
                page_io_task_processor::process_sync(
                    &mut page_io_task_receiver,
                    &page_manager_data_clone,
                );
            })),
            page_io_task_sender,
            page_manager_data,
        })
    }
}

impl Drop for PageManager {
    #[inline]
    fn drop(&mut self) {
        loop {
            match self.page_io_task_sender.try_send(PageIOTask::Shutdown) {
                Ok(_) | Err(TrySendError::Disconnected(_)) => break,
                _ => (),
            }
        }
        if let Some(worker) = self.page_io_worker.take() {
            drop(worker.join());
        }
    }
}
