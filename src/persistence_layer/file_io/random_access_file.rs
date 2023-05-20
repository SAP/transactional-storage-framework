// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

//! Abstraction over an operating system file for random access operations.

#[cfg(unix)]
mod unix {
    use std::fs::{File, Metadata};
    use std::io::Result;
    use std::os::unix::fs::FileExt;
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
        pub fn from_file(file: File, metadata: &Metadata) -> RandomAccessFile {
            RandomAccessFile {
                file,
                len: AtomicU64::new(metadata.len()),
            }
        }

        /// Returns the current length of the file.
        #[inline]
        pub fn len(&self, order: Ordering) -> u64 {
            self.len.load(order)
        }

        /// Truncates or extends the underlying file.
        #[inline]
        pub fn set_len(&self, len: u64) -> Result<()> {
            self.file.set_len(len)?;
            self.len.store(len, Release);
            Ok(())
        }

        /// Synchronizes the data and metadata with the device.
        #[inline]
        pub fn sync_all(&self) -> Result<()> {
            self.file.sync_all()
        }

        /// Abstraction over random read operations.
        #[inline]
        pub fn read(&self, buffer: &mut [u8], offset: u64) -> Result<()> {
            self.file.read_exact_at(buffer, offset)
        }

        /// Abstraction over random write operations.
        #[inline]
        pub fn write(&self, buffer: &[u8], offset: u64) -> Result<()> {
            let result = self.file.write_all_at(buffer, offset);
            if result.is_ok() {
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
            }
            result
        }
    }
}

#[cfg(unix)]
pub use unix::RandomAccessFile;

#[cfg(windows)]
mod windows {
    use std::fs::{File, Metadata};
    use std::io::{Error, ErrorKind, Result};
    use std::os::windows::fs::FileExt;
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
        pub fn from_file(file: File, metadata: &Metadata) -> RandomAccessFile {
            RandomAccessFile {
                file,
                len: AtomicU64::new(metadata.len()),
            }
        }

        /// Returns the current length of the file.
        #[inline]
        pub fn len(&self, order: Ordering) -> u64 {
            self.len.load(order)
        }

        /// Truncates or extends the underlying file.
        #[inline]
        pub fn set_len(&self, len: u64) -> Result<()> {
            self.file.set_len(len)?;
            self.len.store(len, Release);
            Ok(())
        }

        /// Synchronizes the data and metadata with the device.
        #[inline]
        pub fn sync_all(&self) -> Result<()> {
            self.file.sync_all()
        }

        /// Abstraction over random read operations.
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
                Err(Error::from(ErrorKind::UnexpectedEof))
            } else {
                Ok(())
            }
        }

        /// Abstraction over random write operations.
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
            let mut current_len = self.len.load(Relaxed);
            while current_len < offset {
                match self
                    .len
                    .compare_exchange(current_len, offset, Release, Relaxed)
                {
                    Ok(_) => break,
                    Err(actual) => current_len = actual,
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
    use std::fs::{remove_file, OpenOptions};
    use std::io;
    use std::sync::atomic::Ordering::Relaxed;

    #[test]
    fn write_read() {
        const FILE: &str = "random_access_file_write_read_test";
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(FILE)
            .unwrap();
        let metadata = file.metadata().unwrap();
        let random_access_file = RandomAccessFile::from_file(file, &metadata);
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
            random_access_file
                .read(&mut read_buffer, 40)
                .map_err(|e| e.kind()),
            Err(io::ErrorKind::UnexpectedEof)
        );

        drop(random_access_file);
        assert!(remove_file(FILE).is_ok());
    }
}
