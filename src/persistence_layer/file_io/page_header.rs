// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

//! The header of a page.

use super::evictable_page::PageBuffer;

/// The header of a page.
///
/// The layout of a page is as follows.
/// `|8-bit: type|data|`.
#[derive(Debug)]
pub struct PageHeader {
    /// The type of the page.
    ///
    /// The first eight bits of the page represent the type of the page.
    pub page_type: PageType,
}

#[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum PageType {
    /// The page is free.
    Free,

    /// The page is for logging.
    Log,

    /// The page contains a list of other page identifiers.
    Directory,

    /// The page contains data.
    Data,
}

impl PageHeader {
    /// Reads the header from the buffer.
    ///
    /// It writes the header information into the file if none present.
    #[allow(dead_code)]
    #[inline]
    pub fn from_buffer(buffer: &PageBuffer) -> Self {
        let page_type = buffer[0];
        let page_type = match page_type {
            0 => PageType::Free,
            1 => PageType::Log,
            2 => PageType::Directory,
            3 => PageType::Data,
            _ => unreachable!(),
        };
        Self { page_type }
    }

    /// Flushes the content of the [`PageHeader`] to the page buffer.
    #[allow(dead_code)]
    #[inline]
    pub fn flush_header(&self, buffer: &mut PageBuffer) {
        buffer[0] = match self.page_type {
            PageType::Free => 0,
            PageType::Log => 1,
            PageType::Directory => 2,
            PageType::Data => 3,
        };
    }
}
