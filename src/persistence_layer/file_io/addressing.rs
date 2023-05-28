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
    /// Returns the offset in a page.
    #[inline]
    pub const fn page_offset(self) -> u64 {
        self.offset & ((1_u64 << PAGE_BITS) - 1)
    }

    /// Returns the page address.
    #[inline]
    pub const fn page_address(self) -> Address {
        Self {
            offset: self.offset & (!self.page_offset()),
        }
    }

    /// Returns the segment address.
    #[inline]
    pub const fn segment_address(self) -> Address {
        Self {
            offset: self.offset & (!((1_u64 << (SEGMENT_BITS + PAGE_BITS)) - 1)),
        }
    }

    /// Returns the segment directory address.
    #[inline]
    pub const fn segment_directory_address(self) -> Address {
        Self {
            offset: self.offset & (!(SEGMENT_ALIGNMENT - 1)),
        }
    }

    /// Returns the index number of the associated segment.
    #[inline]
    pub const fn segment_directory_index_number(self) -> u64 {
        (self.segment_address().offset - self.segment_directory_address().offset) / SEGMENT_SIZE
    }
}

impl From<u64> for Address {
    #[inline]
    fn from(offset: u64) -> Self {
        Self { offset }
    }
}

impl From<Address> for u64 {
    #[inline]
    fn from(value: Address) -> Self {
        value.offset
    }
}

/// The number of bits to address every byte in a page.
pub const PAGE_BITS: u32 = 9;

/// The size of a page which is `512B`.
pub const PAGE_SIZE: u64 = 1_u64 << PAGE_BITS;

/// The number of bits to address every page in a segment.
pub const SEGMENT_BITS: u32 = 12;

/// The size of a segment is `2MB`, and each segment contains `4096` pages.
///
/// The first bit of a segment is always `0` until the whole segment is permanently deleted from
/// the database.
pub const SEGMENT_SIZE: u64 = 1_u64 << (SEGMENT_BITS + PAGE_BITS);

/// The number of pages in a segment which is `4096`.
pub const PAGES_PER_SEGMENT: usize = (SEGMENT_SIZE / PAGE_SIZE) as usize;

/// The size of a segment directory is `2MB` enabling it to address `8GB` space since a segment
/// addresses `4096` pages where each page is `512B`.
///
/// The first page that follows the first segment directory is the database header. Segment
/// directories are always located at `8GB` boundaries.
///
/// The first bit of a segment directory is always `0` until the whole segments and the segment
/// directory are permanently deleted from the database.
pub const SEGMENT_DIRECTORY_BITS: u32 = SEGMENT_BITS + PAGE_BITS;

/// The size of a segment directory.
pub const SEGMENT_DIRECTORY_SIZE: u64 = 1_u64 << SEGMENT_DIRECTORY_BITS;

/// The alignment of a segment directory.
pub const SEGMENT_ALIGNMENT: u64 = SEGMENT_DIRECTORY_SIZE * (u8::BITS as u64) * PAGE_SIZE;

#[cfg(test)]
mod tests {
    use super::*;
    use static_assertions::const_assert_eq;

    const_assert_eq!(SEGMENT_SIZE, 2_097_152);
    const_assert_eq!(SEGMENT_DIRECTORY_SIZE, 2_097_152);
    const_assert_eq!(SEGMENT_SIZE, SEGMENT_DIRECTORY_SIZE);
    const_assert_eq!(SEGMENT_ALIGNMENT, 8_589_934_592);

    #[test]
    fn in_page_addressing() {
        let address = Address::from(u64::MAX);
        assert_eq!(address.page_offset(), 0b1_1111_1111);
    }

    #[test]
    fn in_segment_addressing() {
        let address = Address::from(u64::from(u32::MAX));
        assert_eq!(
            address.page_address().offset,
            0b1111_1111_1111_1111_1111_1110_0000_0000
        );
    }

    #[test]
    fn in_database_addressing() {
        let address = Address::from(u64::from(u32::MAX));
        assert_eq!(
            address.segment_address().offset,
            0b1111_1111_1110_0000_0000_0000_0000_0000
        );
    }

    #[test]
    fn segment_directory_addressing() {
        let address = Address::from(u64::from(u32::MAX) * 16);
        assert_eq!(
            address.segment_directory_address().offset,
            0b1110_0000_0000_0000_0000_0000_0000_0000_0000
        );
        assert_eq!(
            address.segment_directory_address().offset % SEGMENT_ALIGNMENT,
            0
        );
    }
}
