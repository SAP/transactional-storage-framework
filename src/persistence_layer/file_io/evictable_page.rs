// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

//! Persistent page implementation.

use super::addressing::PAGE_SIZE;
use super::random_access_file::RandomAccessFile;
use crate::Error;
use std::mem::MaybeUninit;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::Relaxed;

/// The in-memory representation of a persistent page.
#[derive(Debug)]
pub struct EvictablePage {
    /// The content of the page.
    ///
    /// The first bit being `0` for a special page represents a special state of the page.
    /// - If the page is a segment directory header, all the segments were deleted.
    /// - If the page is a segment, all the pages were deleted from the database.
    ///
    /// The last bit being marked `1` for a normal page represents a deleted state of the page.
    page_buffer: PageBuffer,

    /// Dirty flag.
    dirty: AtomicBool,
}

/// The type of a page buffer.
#[allow(clippy::cast_possible_truncation)]
pub type PageBuffer = Box<[u8; PAGE_SIZE as usize]>;

/// The index value of the last byte in a page buffer.
#[allow(clippy::cast_possible_truncation)]
const LAST_BYTE_INDEX: usize = (PAGE_SIZE as usize) - 1;

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
        db.read(page_buffer.as_mut_slice(), offset)?;

        Ok(Self {
            page_buffer,
            dirty: AtomicBool::new(false),
        })
    }

    /// Returns `true` if the first bit is marked `1`.
    #[inline]
    pub fn is_first_bit_set(&self) -> bool {
        (self.page_buffer[0] & 1_u8) == 1_u8
    }

    /// Sets the first bit.
    #[allow(dead_code)]
    #[inline]
    pub fn set_first_bit(&mut self) {
        self.page_buffer[0] |= 1_u8;
    }

    /// Unsets the first bit.
    #[allow(dead_code)]
    #[inline]
    pub fn unset_first_bit(&mut self) {
        self.page_buffer[0] &= 1_u8;
    }

    /// Returns `true` if the last bit is marked `1`.
    #[allow(dead_code)]
    #[inline]
    pub fn is_last_bit_set(&self) -> bool {
        (self.page_buffer[LAST_BYTE_INDEX] & (1_u8 << 7)) != 0
    }

    /// Sets the first bit.
    #[allow(dead_code)]
    #[inline]
    pub fn set_last_bit(&mut self) {
        self.page_buffer[LAST_BYTE_INDEX] |= 1_u8 << 7;
    }

    /// Unsets the first bit.
    #[allow(dead_code)]
    #[inline]
    pub fn unset_last_bit(&mut self) {
        self.page_buffer[LAST_BYTE_INDEX] &= 1_u8 << 7;
    }

    /// Gets a reference to the buffer.
    #[allow(dead_code)]
    #[inline]
    pub fn buffer(&self) -> &[u8] {
        self.page_buffer.as_slice()
    }

    /// gets a mutable reference to the buffer.
    #[inline]
    pub fn buffer_mut(&mut self) -> &mut [u8] {
        self.page_buffer.as_mut_slice()
    }

    /// Sets the page dirty.
    #[inline]
    pub fn set_dirty(&mut self) {
        self.dirty.store(true, Relaxed);
    }

    /// Writes back the page buffer to the file.
    ///
    /// # Errors
    ///
    /// Returns an error if writing back the content failed.
    #[inline]
    pub fn write_back(&mut self, db: &RandomAccessFile, offset: u64) -> Result<(), Error> {
        db.write(self.page_buffer.as_slice(), offset)?;
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
