// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

//! Persistent page implementation.

use super::db_header::PAGE_SIZE;
use super::random_access_file::RandomAccessFile;
use crate::Error;
use std::mem::MaybeUninit;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::Relaxed;

/// The in-memory representation of a persistent page.
#[derive(Debug)]
pub struct EvictablePage {
    /// The type of the page.
    page_buffer: PageBuffer,

    /// Dirty flag.
    dirty: AtomicBool,
}

/// The type of a page buffer.
#[allow(clippy::cast_possible_truncation)]
pub type PageBuffer = Box<[u8; PAGE_SIZE as usize]>;

impl EvictablePage {
    /// Creates an [`EvictablePage`] from a file.
    ///
    /// It assumes that the page is not cached anywhere in the system, otherwise it leads to
    /// a multiple versions of the same page problem.
    ///
    /// TODO: it is a blocking system call, therefore need to replace it with an AIO lib.
    #[inline]
    pub fn from_file(db: &RandomAccessFile, offset: u64) -> Result<EvictablePage, Error> {
        #[allow(clippy::uninit_assumed_init, invalid_value)]
        let mut page_buffer: PageBuffer = Box::new(
            // Safety: the buffer will be filled by the following file IO.
            unsafe { MaybeUninit::uninit().assume_init() },
        );

        db.read(page_buffer.as_mut_slice(), offset)
            .map_err(|e| Error::IO(e.kind()))?;

        Ok(Self {
            page_buffer,
            dirty: AtomicBool::new(false),
        })
    }

    /// Reads the content of the page.
    #[inline]
    pub fn read<R, F: FnOnce(&[u8]) -> R>(&self, reader: F) -> R {
        reader(self.page_buffer.as_slice())
    }

    /// Writes data to the page.
    #[inline]
    pub fn write<R, F: FnOnce(&mut [u8]) -> R>(&mut self, writer: F) -> R {
        self.dirty.store(true, Relaxed);
        writer(self.page_buffer.as_mut_slice())
    }

    /// Evicts the page.
    ///
    /// # Errors
    ///
    /// Returns an error if writing back the content failed.
    #[inline]
    pub fn evict(&mut self, db: &RandomAccessFile, offset: u64) -> Result<(), Error> {
        self.write_back(db, offset)
    }

    /// Writes back the buffer.
    fn write_back(&mut self, db: &RandomAccessFile, offset: u64) -> Result<(), Error> {
        db.write(self.page_buffer.as_slice(), offset)
            .map_err(|e| Error::IO(e.kind()))?;
        self.dirty.store(false, Relaxed);
        Ok(())
    }
}

impl Drop for EvictablePage {
    #[inline]
    fn drop(&mut self) {
        // Dropping a dirty page in use is illegal.
        debug_assert!(!self.dirty.load(Relaxed));
    }
}
