// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

//! Persistent page implementation.

use super::addressing::PAGE_SIZE;
use super::random_access_file::RandomAccessFile;
use crate::Error;
use std::mem::MaybeUninit;

/// The in-memory representation of a persistent page.
///
/// The layout of the header of a page is as follows.
/// - `PREV_OFFSET 64-bit|NEXT_OFFSET 64-bit`.
///
/// The layout suggests that a database consists of linked list of pages, and the `PREV_OFFSET`
/// field represents the state of a page.
/// - `NULL|*`: the page is unreachable, and will be eventually added to a free page list.
/// - `PREV_OFFSET|*`: the page is reachable.
///
/// Creating a new page entails linking the new page to an existing page, which requires one
/// synchronous IO.
/// 1. Synchronously set the address of the existing page to the `PREV_OFFSET` field.
/// - The page to create cannot be owned by any other transaction at runtime.
/// - If the server crashes, the page is regarded as free if the previous page does not point to
///   it, and the page will be eventually added to a free page list.
/// 2. Set the `NEXT_OFFSET` field of the existing page to the address of the new page.
///
/// Deleting an existing page from the linked list requires one synchronous IO.
/// 1. Synchronously reset the `PREV_OFFSET` field of the page to delete.
/// - The page to delete cannot be owned by any other transaction at runtime.
/// - If the server crashes, the linked list state is fixed during recovery; this inconsistency can
///   be easily detected by back-tracking `PREV_OFFSET` values.
/// 2. Set the `NEXT_OFFSET` field of the previous page to the `NEXT_OFFSET` field value of the
///    page to delete.
/// 3. Set the `PREV_OFFSET` field of the next page to the address of the previous page.
/// 4. After all the operations above have been persisted, the page can be inserted into a free
///    page list.
#[derive(Debug)]
pub struct EvictablePage {
    /// The offset in the file and the dirty flag of the page.
    ///
    /// The layout of the field is `offset: 63-bit|dirty_flag: 1-bit`.
    offset_and_dirty_flag: u64,

    /// The content of the page.
    ///
    /// The first `16B` is reserved for the header of the page.
    page_buffer: PageBuffer,
}

/// The type of a page buffer.
#[allow(clippy::cast_possible_truncation)]
pub type PageBuffer = [u8; PAGE_SIZE as usize];

/// The length of the page header of a page.
pub const PAGE_HEADER_LEN: usize = 16;

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
        // Safety: the buffer will be filled by the following file IO.
        let mut page_buffer: PageBuffer = unsafe { MaybeUninit::uninit().assume_init() };
        db.read(page_buffer.as_mut_slice(), offset)?;

        Ok(Self {
            offset_and_dirty_flag: offset,
            page_buffer,
        })
    }

    /// Returns `true` if the page is dirty.
    #[inline]
    pub fn is_dirty(&self) -> bool {
        (self.offset_and_dirty_flag & 1_u64) != 0
    }

    /// Sets the page dirty.
    #[allow(dead_code)]
    #[inline]
    pub fn set_dirty(&mut self) {
        self.offset_and_dirty_flag |= 1_u64;
    }

    /// Gets a reference to the buffer.
    #[allow(dead_code)]
    #[inline]
    pub fn buffer(&self) -> &[u8] {
        &self.page_buffer[PAGE_HEADER_LEN..]
    }

    /// Gets a mutable reference to the buffer.
    #[allow(dead_code)]
    #[inline]
    pub fn buffer_mut(&mut self) -> &mut [u8] {
        &mut self.page_buffer[PAGE_HEADER_LEN..]
    }

    /// Writes back the page buffer to the file.
    ///
    /// # Errors
    ///
    /// Returns an error if writing back the content failed.
    #[inline]
    pub fn write_back(&mut self, db: &RandomAccessFile) -> Result<(), Error> {
        db.write(
            self.page_buffer.as_slice(),
            self.offset_and_dirty_flag & (!1_u64),
        )?;
        self.offset_and_dirty_flag &= !1_u64;
        Ok(())
    }
}

impl Drop for EvictablePage {
    #[inline]
    fn drop(&mut self) {
        // Dropping a dirty page in use is illegal.
        debug_assert!(!self.is_dirty());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use static_assertions::assert_eq_size;

    assert_eq_size!(EvictablePage, [u64; 65]);
}
