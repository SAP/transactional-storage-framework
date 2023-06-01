// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

//! The module defines constants and methods to correctly interpret offset values in a database
//! file.

#![allow(dead_code)]

/// [`Address`] is an offset in a database file.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn in_page_addressing() {
        let address = Address::from(u64::MAX);
        assert_eq!(address.page_offset(), 0b1_1111_1111);
    }
}
