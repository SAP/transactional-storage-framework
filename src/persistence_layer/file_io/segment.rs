// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

//! [`Segment`] is a special type of database pages that manages a group of consecutive pages.

#![allow(dead_code)]

use super::addressing::PAGES_PER_SEGMENT;

/// [`Segment`] is a special type of database pages that manages a group of consecutive pages that
/// follow the [`Segment`] page in the corresponding database file.
#[derive(Debug, Default)]
pub struct Segment {
    /// Free page bitmap.
    ///
    /// The bitmap is able to address `4096` pages.
    free_page_bitmap: [[u64; BITMAP_ARRAY_LEN]; 2],
}

/// The size of the bitmap in a [`Segment`] which is `32`.
pub const BITMAP_ARRAY_LEN: usize = (PAGES_PER_SEGMENT / (u64::BITS as usize)) / 2;

#[cfg(test)]
mod tests {
    use super::*;
    use static_assertions::const_assert_eq;

    const_assert_eq!(PAGES_PER_SEGMENT, 4_096);
    const_assert_eq!(BITMAP_ARRAY_LEN, 32);
}
