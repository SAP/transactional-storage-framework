// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use crate::{AtomicCounter, Log, Snapshot, Version, VersionCell};

use std::sync::atomic::Ordering::Relaxed;

use scc::ebr;

pub struct RecordVersion {
    version_cell: ebr::AtomicArc<VersionCell<AtomicCounter>>,
}

impl Default for RecordVersion {
    fn default() -> Self {
        RecordVersion {
            version_cell: ebr::AtomicArc::new(VersionCell::default()),
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

    fn version_cell_ptr<'b>(
        &self,
        barrier: &'b ebr::Barrier,
    ) -> ebr::Ptr<'b, VersionCell<AtomicCounter>> {
        self.version_cell.load(Relaxed, barrier)
    }

    fn write(&mut self, _payload: RecordVersion) -> Option<Log> {
        None
    }

    fn read(&self, _snapshot: &Snapshot<AtomicCounter>) -> Option<&RecordVersion> {
        None
    }

    fn consolidate(&self, barrier: &ebr::Barrier) -> bool {
        if let Some(version_cell) = self.version_cell.swap((None, ebr::Tag::None), Relaxed) {
            barrier.reclaim(version_cell);
            return true;
        }
        false
    }
}
