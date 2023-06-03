// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

//! The header of the database file.

use super::evictable_page::{EvictablePage, PAGE_HEADER_LEN, PAGE_SIZE};
use super::RandomAccessFile;
use crate::Error;
use scc::Bag;
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
}

/// The current database version.
pub const VERSION: u64 = 1;

impl DatabaseHeader {
    /// Reads the header from the database file.
    ///
    /// It writes the header information into the file if none present.
    #[inline]
    pub fn from_file(db: &RandomAccessFile) -> Result<Self, Error> {
        if db.len(Relaxed) == 0 {
            // The file is empty, creating a new database.
            db.set_len(PAGE_SIZE * 4)?;
            let version = VERSION.to_le_bytes();
            db.write(&version, PAGE_HEADER_LEN as u64)?;
            let log_head = PAGE_SIZE;
            db.write(&log_head.to_le_bytes(), PAGE_HEADER_LEN as u64 + 8)?;
            let container_directory_head = PAGE_SIZE * 2;
            db.write(
                &container_directory_head.to_le_bytes(),
                PAGE_HEADER_LEN as u64 + 16,
            )?;

            // The fourth page is initially free.
            let free_pages = Bag::new();
            free_pages.push(PAGE_SIZE * 3);

            Ok(Self {
                version: VERSION,
                log_head,
                container_directory_head,
                free_pages,
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
            })
        }
    }
}
