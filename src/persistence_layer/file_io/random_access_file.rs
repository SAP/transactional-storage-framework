// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

//! Abstraction of a file for POSIX operating systems and Windows.

#![allow(dead_code)]

#[cfg(unix)]
mod unix {
    use std::fs::File;
    use std::io::Result;
    use std::os::unix::fs::FileExt;

    /// [`RandomAccessFile`] allows the user to freely read and write any random location of the
    /// [`File`].
    pub struct RandomAccessFile {
        file: File,
    }

    impl RandomAccessFile {
        /// Creates a new [`RandomAccessFile`].
        #[inline]
        pub fn from_file(file: File) -> RandomAccessFile {
            RandomAccessFile { file }
        }

        /// Abstraction of random read operations.
        #[inline]
        pub fn read(&self, buffer: &mut [u8], offset: u64) -> Result<()> {
            self.file.read_exact_at(buffer, offset)
        }

        /// Abstraction of random write operations.
        #[inline]
        pub fn write(&self, buffer: &[u8], offset: u64) -> Result<()> {
            self.file.write_all_at(buffer, offset)
        }
    }
}

#[cfg(unix)]
pub use unix::RandomAccessFile;

#[cfg(windows)]
mod windows {
    use std::fs::File;
    use std::io::Result;
    use std::io::{Error, ErrorKind};
    use std::os::windows::fs::FileExt;

    /// [`RandomAccessFile`] allows the user to freely read and write any random location of the
    /// [`File`].
    pub struct RandomAccessFile {
        file: File,
    }

    impl RandomAccessFile {
        /// Creates a new [`RandomAccessFile`].
        #[inline]
        pub fn from_file(file: File) -> RandomAccessFile {
            RandomAccessFile { file }
        }

        /// Abstraction of random read operations.
        #[inline]
        pub fn read(&self, mut buffer: &mut [u8], mut offset: u64) -> Result<()> {
            while !buffer.is_empty() {
                match self.file.seek_read(buffer, offset) {
                    Ok(0) => break,
                    Ok(n) => {
                        let tmp = buffer;
                        buffer = &mut tmp[n..];
                        offset += n as u64;
                    }
                    Err(ref e) if e.kind() == ErrorKind::Interrupted => {}
                    Err(e) => return Err(e),
                }
            }
            if !buffer.is_empty() {
                Err(Error::new(
                    ErrorKind::UnexpectedEof,
                    "failed to fill whole buffer",
                ))
            } else {
                Ok(())
            }
        }

        /// Abstraction of random write operations.
        #[inline]
        pub fn write(&self, mut buffer: &[u8], mut offset: u64) -> Result<()> {
            while !buffer.is_empty() {
                match self.file.seek_write(buffer, offset) {
                    Ok(0) => {
                        return Err(Error::new(
                            ErrorKind::WriteZero,
                            "failed to write whole buffer",
                        ));
                    }
                    Ok(n) => {
                        buffer = &buffer[n..];
                        offset += n as u64
                    }
                    Err(ref e) if e.kind() == ErrorKind::Interrupted => {}
                    Err(e) => return Err(e),
                }
            }
            Ok(())
        }
    }
}

#[cfg(windows)]
pub use windows::RandomAccessFile;

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs::{remove_file, OpenOptions},
        io,
    };

    #[test]
    fn write_read() {
        const FILE: &str = "random_access_file_write_read_test";
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(FILE)
            .unwrap();
        let random_access_file = RandomAccessFile::from_file(file);
        let mut write_buffer: [u8; 32] = [0; 32];
        for d in 0..32_u8 {
            write_buffer[d as usize] = d;
        }
        assert!(random_access_file.write(&write_buffer, 18).is_ok());

        let mut read_buffer: [u8; 16] = [0; 16];
        assert!(random_access_file.read(&mut read_buffer, 18).is_ok());

        for d in 0..16_u8 {
            assert_eq!(read_buffer[d as usize], d);
        }

        assert_eq!(
            random_access_file
                .read(&mut read_buffer, 40)
                .map_err(|e| e.kind()),
            Err(io::ErrorKind::UnexpectedEof)
        );

        drop(random_access_file);
        assert!(remove_file(FILE).is_ok());
    }
}
