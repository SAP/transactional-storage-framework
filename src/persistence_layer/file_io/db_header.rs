// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

//! The header of the database file.

use super::RandomAccessFile;
use std::sync::atomic::Ordering::Relaxed;

/// The header of the database file.
#[derive(Debug)]
pub struct DatabaseHeader {
    /// The log file.
    pub log_file: LogFile,
}

#[derive(Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub enum LogFile {
    /// `0.log` in the same directory.
    #[default]
    Zero,

    /// `1.log` in the same directory.
    One,
}

impl DatabaseHeader {
    /// Reads the header from the database file.
    pub fn from_file(db: &RandomAccessFile) -> Self {
        let mut buffer = [0_u8; 8];
        if db.len(Relaxed) == 0 {
            Self {
                log_file: LogFile::default(),
            }
        } else {
            db.read(&mut buffer, 0).unwrap();
            let log_file = if buffer[0] == 0 {
                LogFile::Zero
            } else {
                LogFile::One
            };
            Self { log_file }
        }
    }
}
