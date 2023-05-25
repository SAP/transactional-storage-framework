// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

//! [`Segment`] is a special type of database pages that manages a group of consecutive pages.

#![allow(dead_code)]

/// [`Segment`] is a special type of database pages that manages a group of consecutive pages that
/// follow the [`Segment`] page in the corresponding database file.
///
/// The offset of a [`Segment`] is always a multiple of [`SEGMENT_SIZE`] which is `1-megabyte` by
/// default.
#[derive(Debug, Default)]
pub struct Segment {
    /// Free page bitmap.
    ///
    /// The bitmap is able to manage `2048` pages, and the size of the bitmap is `256-byte`.
    free_pages: [u64; BITMAP_ARRAY_LEN],

    /// The offset in the database file.
    offset: u64,

    /// The type of the pages in the [`Segment`].
    page_type: PageType,
}

/// The type of pages in a [`Segment`].
#[derive(Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub enum PageType {
    /// The pages in the segment contains data.
    #[default]
    Data,

    /// The pages in the segment are for log records.
    Log,
}

/// The size of a page which is `512-byte`.
pub const PAGE_SIZE: u64 = 1_u64 << 9;

/// The size of a [`Segment`] which is `1-megabyte`.
pub const SEGMENT_SIZE: u64 = 1_u64 << 20;

/// The number of pages in a [`Segment`] which is `2048`.
pub const PAGES_PER_SEGMENT: usize = (SEGMENT_SIZE / PAGE_SIZE) as usize;

/// The size of the bitmap in a [`Segment`] which is `32`.
pub const BITMAP_ARRAY_LEN: usize = PAGES_PER_SEGMENT / (u64::BITS as usize);

#[cfg(test)]
mod tests {
    use super::*;
    use static_assertions::const_assert;

    const_assert!(SEGMENT_SIZE == 1_048_576);
    const_assert!(PAGES_PER_SEGMENT == 2_048);
    const_assert!(BITMAP_ARRAY_LEN == 32);
}
