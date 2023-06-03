// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

//! The header of the database file.

use super::evictable_page::{EvictablePage, PAGE_HEADER_LEN, PAGE_SIZE};
use super::RandomAccessFile;
use crate::Error;
use scc::Bag;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;

/// The header of the database file that occupies the first page of the database.
#[derive(Debug)]
pub struct DatabaseHeader {
    /// Database version.
    pub version: u64,

    /// The log page link head.
    pub log_head: u64,

    /// The container directory page link head.
    pub container_directory_head: u64,

    /// The free page set.
    ///
    /// TODO: optimize memory usage, e.g., by using a bit-vector.
    pub free_pages: Bag<u64>,

    /// The current offset of the free page scanner.
    pub free_page_scanner_offset: AtomicU64,
}

/// The current database version.
pub const VERSION: u64 = 1;

/// The address where the log head is located.
const DEFAULT_LOG_HEAD_PAGE: u64 = PAGE_SIZE;

/// The address where the container directory head is located.
const DEFAULT_CONTAINER_DIRECTORY_PAGE: u64 = PAGE_SIZE * 2;

/// The offset where the log container directory head page address if stored.
const DEFAULT_FREE_PAGE: u64 = PAGE_SIZE * 3;

impl DatabaseHeader {
    /// Reads the header from the database file.
    ///
    /// It writes the header information into the file if none present.
    #[inline]
    pub fn from_file(db: &RandomAccessFile) -> Result<Self, Error> {
        if db.len(Relaxed) == 0 {
            // The file is empty, creating a new database.
            db.set_len(PAGE_SIZE * 4)?;

            let base_offset = PAGE_HEADER_LEN as u64;
            db.write(&VERSION.to_le_bytes(), base_offset)?;
            db.write(&DEFAULT_LOG_HEAD_PAGE.to_le_bytes(), base_offset + 8)?;
            db.write(
                &DEFAULT_CONTAINER_DIRECTORY_PAGE.to_le_bytes(),
                base_offset + 16,
            )?;

            // The fourth page is initially free.
            let free_pages = Bag::new();
            free_pages.push(DEFAULT_FREE_PAGE);

            Ok(Self {
                version: VERSION,
                log_head: DEFAULT_LOG_HEAD_PAGE,
                container_directory_head: DEFAULT_CONTAINER_DIRECTORY_PAGE,
                free_pages,
                free_page_scanner_offset: AtomicU64::new(DEFAULT_FREE_PAGE),
            })
        } else {
            let database_page = EvictablePage::from_file(db, 0)?;
            let mut iter = database_page.buffer().chunks(8);
            let version = u64::from_le_bytes(iter.next().unwrap().try_into().unwrap());
            let log_head = u64::from_le_bytes(iter.next().unwrap().try_into().unwrap());
            let container_directory_head =
                u64::from_le_bytes(iter.next().unwrap().try_into().unwrap());
            Ok(Self {
                version,
                log_head,
                container_directory_head,
                free_pages: Bag::new(),
                free_page_scanner_offset: AtomicU64::new(DEFAULT_FREE_PAGE),
            })
        }
    }
}
