// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use crate::version::Cell;
use crate::{AtomicCounter, Log, Snapshot, Version};

use std::sync::atomic::Ordering::Relaxed;

use scc::ebr;

/// [`RecordVersion`] is a versioned database object for a record.
pub struct RecordVersion {
    version_cell: ebr::AtomicArc<Cell<AtomicCounter>>,
}

impl Default for RecordVersion {
    fn default() -> Self {
        RecordVersion {
            version_cell: ebr::AtomicArc::new(Cell::default()),
        }
    }
}
impl RecordVersion {
    /// Creates a new [`RecordVersion`].
    #[must_use]
    pub fn new() -> RecordVersion {
        RecordVersion {
            version_cell: ebr::AtomicArc::null(),
        }
    }
}

impl Version<AtomicCounter> for RecordVersion {
    type Data = RecordVersion;

    fn version_cell_ptr<'b>(&self, barrier: &'b ebr::Barrier) -> ebr::Ptr<'b, Cell<AtomicCounter>> {
        self.version_cell.load(Relaxed, barrier)
    }

    fn write(&mut self, _payload: RecordVersion) -> Option<Log> {
        None
    }

    fn read(
        &self,
        _snapshot: &Snapshot<AtomicCounter>,
        _barrier: &ebr::Barrier,
    ) -> Option<&RecordVersion> {
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
