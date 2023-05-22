// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

//! Persistent page implementation.

#![allow(dead_code)]

use super::{db_header::PAGE_SIZE, random_access_file::RandomAccessFile};
use crate::Error;
use std::mem::MaybeUninit;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::Relaxed;

/// The in-memory representation of a persistent page.
#[derive(Debug)]
pub struct EvictablePage {
    /// The type of the page.
    pub page_type: PageType,

    /// Dirty flag.
    pub dirty: AtomicBool,
}

/// The type of a page buffer.
pub type PageBuffer = Box<[u8; PAGE_SIZE as usize]>;

/// The type of pages.
#[derive(Debug)]
pub enum PageType {
    /// The page is free.
    Free(PageBuffer),

    /// The page is used as log space.
    Log(PageBuffer),

    /// The page is used as a directory.
    ///
    /// A directory is a list of `u64` values where each `u64` value represents the offset in the
    /// database file.
    ///
    /// The `u64` value prepended to the buffer points to the next directory page of the same kind.
    Directory(u64, PageBuffer),

    /// The page is used as raw data container.
    Raw(PageBuffer),
}

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
        let mut buffer: PageBuffer = Box::new(
            // Safety: the buffer will be filled by the following file IO.
            unsafe { MaybeUninit::uninit().assume_init() },
        );

        db.read(buffer.as_mut_slice(), offset)
            .map_err(|e| Error::IO(e.kind()))?;

        // TODO: interpret the page header.
        Ok(Self {
            page_type: PageType::Raw(buffer),
            dirty: AtomicBool::new(false),
        })
    }

    /// Evicts the page.
    ///
    /// # Errors
    ///
    /// Returns an error if writing back the content failed.
    pub fn evict(&mut self, db: &RandomAccessFile, offset: u64) -> Result<(), Error> {
        let buffer = match &self.page_type {
            PageType::Free(buffer)
            | PageType::Log(buffer)
            | PageType::Directory(_, buffer)
            | PageType::Raw(buffer) => buffer,
        };
        Self::write_back(db, buffer, offset)
    }

    /// Writes back the buffer.
    fn write_back(db: &RandomAccessFile, buffer: &PageBuffer, offset: u64) -> Result<(), Error> {
        db.write(buffer.as_slice(), offset)
            .map_err(|e| Error::IO(e.kind()))?;
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
