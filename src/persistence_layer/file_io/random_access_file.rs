// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

//! Abstraction over an operating system file for random access operations.

use crate::Error;
#[cfg(target_os = "linux")]
use libc::O_DIRECT;
use libc::O_SYNC;
use std::fs::{File, OpenOptions};
use std::os::raw::c_int;
use std::os::unix::fs::FileExt;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::{self, Relaxed, Release};

/// [`RandomAccessFile`] allows the user to freely read and write any random location of the
/// [`File`].
#[derive(Debug)]
pub struct RandomAccessFile {
    /// The underlying file handle.
    file: File,

    /// The current length of the file.
    len: AtomicU64,
}

impl RandomAccessFile {
    /// Creates a new [`RandomAccessFile`].
    #[inline]
    pub fn from_file(path: &Path) -> Result<RandomAccessFile, Error> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .custom_flags(custom_flag())
            .open(path)
            .map_err(|e| Error::IO(e.kind()))?;
        let metadata = file.metadata().map_err(|e| Error::IO(e.kind()))?;
        Ok(RandomAccessFile {
            file,
            len: AtomicU64::new(metadata.len()),
        })
    }

    /// Returns the current length of the file.
    #[inline]
    pub fn len(&self, order: Ordering) -> u64 {
        self.len.load(order)
    }

    /// Truncates or extends the underlying file.
    #[inline]
    pub fn set_len(&self, len: u64) -> Result<(), Error> {
        self.file.set_len(len).map_err(|e| Error::IO(e.kind()))?;
        self.len.store(len, Release);
        Ok(())
    }

    /// Abstraction over random read operations.
    #[inline]
    pub fn read(&self, buffer: &mut [u8], offset: u64) -> Result<(), Error> {
        self.file
            .read_exact_at(buffer, offset)
            .map_err(|e| Error::IO(e.kind()))
    }

    /// Abstraction over random write operations.
    #[inline]
    pub fn write(&self, buffer: &[u8], offset: u64) -> Result<(), Error> {
        self.file
            .write_all_at(buffer, offset)
            .map_err(|e| Error::IO(e.kind()))?;
        let mut current_len = self.len.load(Relaxed);
        let new_len = offset + buffer.len() as u64;
        while current_len < new_len {
            match self
                .len
                .compare_exchange(current_len, new_len, Release, Relaxed)
            {
                Ok(_) => break,
                Err(actual) => current_len = actual,
            }
        }
        Ok(())
    }
}

#[cfg(target_os = "linux")]
fn custom_flag() -> c_int {
    // Synchronous direct access to the device.
    O_DIRECT | O_SYNC
}

#[cfg(target_os = "macos")]
fn custom_flag() -> c_int {
    // `O_DIRECT` is unavailable.
    O_SYNC
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::remove_file;
    use std::io;
    use std::sync::atomic::Ordering::Relaxed;

    #[test]
    fn write_read() {
        const FILE: &str = "random_access_file_write_read_test";
        let random_access_file = RandomAccessFile::from_file(Path::new(FILE)).unwrap();
        assert_eq!(random_access_file.len(Relaxed), 0);
        let mut write_buffer: [u8; 32] = [0; 32];
        for d in 0..32_u8 {
            write_buffer[d as usize] = d;
        }
        assert!(random_access_file.write(&write_buffer, 18).is_ok());
        assert_eq!(
            random_access_file.len(Relaxed),
            write_buffer.len() as u64 + 18
        );

        let mut read_buffer: [u8; 16] = [0; 16];
        assert!(random_access_file.read(&mut read_buffer, 18).is_ok());

        for d in 0..16_u8 {
            assert_eq!(read_buffer[d as usize], d);
        }

        assert_eq!(
            random_access_file.read(&mut read_buffer, 40),
            Err(Error::IO(io::ErrorKind::UnexpectedEof))
        );

        drop(random_access_file);
        assert!(remove_file(FILE).is_ok());
    }
}
