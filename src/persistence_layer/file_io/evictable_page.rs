// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

//! Persistent page implementation.

#![allow(dead_code)]

use std::marker::PhantomData;

/// The in-memory representation of a persistent page.
#[derive(Debug)]
pub struct EvictablePage<'d> {
    /// The type of the page.
    pub page_type: PageType,

    _phantom: PhantomData<&'d ()>,
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
}

impl<'d> Drop for EvictablePage<'d> {
    #[inline]
    fn drop(&mut self) {
        // TODO: disk write.
    }
}
