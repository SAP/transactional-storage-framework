// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

//! Persistent page implementation.

#![allow(dead_code)]

/// The in-memory representation of a persistent page.
#[derive(Debug)]
pub struct EvictablePage {
    /// The type of the page.
    pub page_type: PageType,
}

/// The type of pages.
#[derive(Debug)]
pub enum PageType {
    /// The page is free.
    Free,

    /// The page is used as log space.
    Log,

    /// The page is used as a directory.
    ///
    /// A directory is a list of `u64` values where each `u64` value represents the offset in the
    /// database file.
    Directory,

    /// The page is used as raw data container.
    Raw,

    /// The page is evicted from the page cache.
    Evicted,
}

impl Drop for EvictablePage {
    #[inline]
    fn drop(&mut self) {
        // Dropping a page in use is illegal.
        debug_assert!(matches!(self.page_type, PageType::Evicted));
    }
}
