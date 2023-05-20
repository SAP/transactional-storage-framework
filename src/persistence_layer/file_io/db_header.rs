// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

//! The header of the database file.

use super::RandomAccessFile;
use crate::Error;
use std::sync::atomic::Ordering::Relaxed;

/// The header of the database file.
#[derive(Debug)]
pub struct DatabaseHeader {
    /// Version.
    pub version: u64,
}

/// The database version.
pub const VERSION: u64 = 1;

/// The size of a page.
pub const PAGE_SIZE: u64 = 1_u64 << 9;

impl DatabaseHeader {
    /// Reads the header from the database file.
    ///
    /// It writes the header information into the file if none present.
    pub fn from_file(db: &RandomAccessFile) -> Result<Self, Error> {
        let mut buffer = [0_u8; 8];
        if db.len(Relaxed) == 0 {
            buffer = VERSION.to_le_bytes();
            db.set_len(PAGE_SIZE).map_err(|e| Error::IO(e.kind()))?;
            db.write(&mut buffer, 0).map_err(|e| Error::IO(e.kind()))?;
            db.sync_all().map_err(|e| Error::IO(e.kind()))?;
            Ok(Self { version: VERSION })
        } else {
            db.read(&mut buffer, 0).map_err(|e| Error::IO(e.kind()))?;
            let version = u64::from_le_bytes(buffer);
            Ok(Self { version })
        }
    }
}
