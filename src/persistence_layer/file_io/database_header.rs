// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

//! The header of the database file.

use super::addressing::{PAGE_SIZE, SEGMENT_DIRECTORY_SIZE};
use super::RandomAccessFile;
use crate::Error;
use std::sync::atomic::Ordering::Relaxed;

/// The header of the database file.
#[derive(Debug)]
pub struct DatabaseHeader {
    /// Database version.
    pub version: u64,
}

/// The current database version.
pub const VERSION: u64 = 1;

impl DatabaseHeader {
    /// Reads the header from the database file.
    ///
    /// It writes the header information into the file if none present.
    #[inline]
    pub fn from_file(db: &RandomAccessFile) -> Result<Self, Error> {
        let mut buffer = [0_u8; 8];
        if db.len(Relaxed) == 0 {
            // The file is empty, creating a new database.
            db.set_len(SEGMENT_DIRECTORY_SIZE + PAGE_SIZE)
                .map_err(|e| Error::IO(e.kind()))?;
            buffer = VERSION.to_le_bytes();
            db.write(&buffer, SEGMENT_DIRECTORY_SIZE)
                .map_err(|e| Error::IO(e.kind()))?;
            db.sync_all().map_err(|e| Error::IO(e.kind()))?;
            Ok(Self { version: VERSION })
        } else {
            db.read(&mut buffer, SEGMENT_DIRECTORY_SIZE)
                .map_err(|e| Error::IO(e.kind()))?;
            let version = u64::from_le_bytes(buffer);
            Ok(Self { version })
        }
    }
}
