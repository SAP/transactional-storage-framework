// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

//! The module defines constants and methods to correctly interpret offset values in a database
//! file.

#![allow(dead_code)]

/// [`Address`] is an offset in a database file.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct Address {
    /// The offset in a database file that the [`Address`] is pointing to.
    offset: u64,
}

impl Address {
    /// The offset in a page.
    #[inline]
    pub const fn page_offset(self) -> u64 {
        self.offset & ((1_u64 << PAGE_BITS) - 1)
    }

    /// The page number in a segment.
    #[inline]
    pub const fn page_number(self) -> u64 {
        (self.offset >> PAGE_BITS) & ((1_u64 << SEGMENT_BITS) - 1)
    }
}

impl From<u64> for Address {
    #[inline]
    fn from(offset: u64) -> Self {
        Self { offset }
    }
}

/// The number of bits to address every byte in a page.
pub const PAGE_BITS: u32 = 9;

/// The size of a page which is `512B`.
pub const PAGE_SIZE: u64 = 1_u64 << PAGE_BITS;

/// The number of bits to address every page in a segment.
pub const SEGMENT_BITS: u32 = 12;

/// The size of a segment is `2MB`, and each segment contains `4096` pages.
pub const SEGMENT_SIZE: u64 = 1_u64 << (SEGMENT_BITS + PAGE_BITS);

/// The number of pages in a segment which is `4096`.
pub const PAGES_PER_SEGMENT: usize = (SEGMENT_SIZE / PAGE_SIZE) as usize;

/// The size of a segment directory is `4MB` enabling it to address `16GB` space since a segment
/// addresses `4096` pages where each page is `512B`.
///
/// The first page that follows the first segment directory is the database header. Segment
/// directories are always located at `16GB` boundaries.
pub const SEGMENT_DIRECTORY_BITS: u32 = 22;

/// The size of a segment directory.
pub const SEGMENT_DIRECTORY_SIZE: u64 = 1_u64 << SEGMENT_DIRECTORY_BITS;

/// The alignment of a segment directory.
pub const SEGMENT_ALIGNMENT: u64 = SEGMENT_DIRECTORY_SIZE * (u8::BITS as u64) * PAGE_SIZE;

#[cfg(test)]
mod tests {
    use super::*;
    use static_assertions::const_assert_eq;

    const_assert_eq!(SEGMENT_SIZE, 2_097_152);
    const_assert_eq!(SEGMENT_DIRECTORY_SIZE, 4_194_304);
    const_assert_eq!(SEGMENT_ALIGNMENT, 17_179_869_184);

    #[test]
    fn in_page_addressing() {
        let address = Address::from(u64::MAX);
        assert_eq!(address.page_offset(), 0b1_1111_1111);
    }

    #[test]
    fn in_segment_addressing() {
        let address = Address::from(u64::MAX);
        assert_eq!(address.page_number(), 0b1111_1111_1111);
    }
}
