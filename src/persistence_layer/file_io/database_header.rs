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
    /// Number of bits to address a segment.
    pub segment_directory_bits: u32,
}

/// The default size of a segment directory is `16MB`.
///
/// The database header is `1B` which is able to address eight segments where the size of a segment
/// is `2MB`. The first eight segments area is thus used for a segment directory by default.
///
/// The default segment directory can address `256GB`.
pub const DEFAULT_SEGMENT_DIRECTORY_BITS: u32 = 24;

impl DatabaseHeader {
    /// Returns the size of the segment directory.
    #[inline]
    pub const fn segment_directory_size(&self) -> u64 {
        1_u64 << self.segment_directory_bits
    }

    /// Reads the header from the database file.
    ///
    /// It writes the header information into the file if none present.
    #[inline]
    pub fn from_file(db: &RandomAccessFile) -> Result<Self, Error> {
        // The first 1-byte contains the number of bits to use for segment directory addressing.
        let mut buffer = [0_u8; 1];
        if db.len(Relaxed) == 0 {
            let default_db_header = Self {
                segment_directory_bits: DEFAULT_SEGMENT_DIRECTORY_BITS,
            };
            let default_segment_directory_size = default_db_header.segment_directory_size();
            db.set_len(default_segment_directory_size)
                .map_err(|e| Error::IO(e.kind()))?;
            {
                #![allow(clippy::cast_possible_truncation)]
                buffer[0] = DEFAULT_SEGMENT_DIRECTORY_BITS as u8;
            }
            db.write(&buffer, 0).map_err(|e| Error::IO(e.kind()))?;
            db.sync_all().map_err(|e| Error::IO(e.kind()))?;
            Ok(default_db_header)
        } else {
            db.read(&mut buffer, 0).map_err(|e| Error::IO(e.kind()))?;
            let segment_directory_bits = u32::from(buffer[0]);
            if segment_directory_bits == 0 || segment_directory_bits > u64::BITS {
                return Err(Error::CorruptDatabase);
            }
            let db_header = Self {
                segment_directory_bits,
            };
            if db.len(Relaxed) < db_header.segment_directory_size() {
                return Err(Error::CorruptDatabase);
            }
            Ok(db_header)
        }
    }
}
