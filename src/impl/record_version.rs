// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use crate::{AtomicCounter, Version, VersionCell};
use crossbeam_epoch::{Atomic, Guard, Shared};
use std::sync::atomic::Ordering::Relaxed;

pub struct RecordVersion {
    version_cell: Atomic<VersionCell<AtomicCounter>>,
}

impl Default for RecordVersion {
    fn default() -> Self {
        RecordVersion {
            version_cell: Atomic::new(VersionCell::new()),
        }
    }
}
impl RecordVersion {
    pub fn new() -> RecordVersion {
        Default::default()
    }
}

impl Version<AtomicCounter> for RecordVersion {
    type Data = RecordVersion;
    fn version_cell<'g>(&self, guard: &'g Guard) -> Shared<'g, VersionCell<AtomicCounter>> {
        self.version_cell.load(Relaxed, guard)
    }
    fn read(&self) -> &RecordVersion {
        self
    }
    fn unversion(&self, guard: &Guard) -> bool {
        !self
            .version_cell
            .swap(Shared::null(), Relaxed, guard)
            .is_null()
    }
}
