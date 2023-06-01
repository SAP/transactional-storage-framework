// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

//! The header of the database file.

use super::addressing::PAGE_SIZE;
use super::evictable_page::{EvictablePage, PAGE_HEADER_LEN};
use super::RandomAccessFile;
use crate::Error;
use std::sync::atomic::Ordering::Relaxed;

/// The header of the database file that occupies the first page of the database.
#[derive(Debug)]
pub struct DatabaseHeader {
    /// Database version.
    pub version: u64,

    /// The log page link.
    pub log: u64,

    /// The container directory page link.
    pub container_directory: u64,

    /// The free page links.
    pub free_pages: [u64; 32],
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
            db.set_len(PAGE_SIZE)?;
            let buffer = VERSION.to_le_bytes();
            db.write(&buffer, PAGE_HEADER_LEN as u64)?;
            Ok(Self {
                version: VERSION,
                log: 0,
                container_directory: 0,
                free_pages: [0_u64; 32],
            })
        } else {
            let database_page = EvictablePage::from_file(db, 0)?;
            let mut iter = database_page.buffer().chunks(8);
            let version = u64::from_le_bytes(iter.next().unwrap().try_into().unwrap());
            let log = u64::from_le_bytes(iter.next().unwrap().try_into().unwrap());
            let container_directory = u64::from_le_bytes(iter.next().unwrap().try_into().unwrap());
            let mut free_pages = [0_u64; 32];
            for f in &mut free_pages {
                *f = u64::from_le_bytes(iter.next().unwrap().try_into().unwrap());
            }
            Ok(Self {
                version,
                log,
                container_directory,
                free_pages,
            })
        }
    }
}
