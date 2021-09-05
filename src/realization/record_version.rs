// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use crate::{AtomicSequencer, Log, Snapshot, Version, VersionCell};

use std::sync::atomic::Ordering::Relaxed;

use scc::ebr;

pub struct RecordVersion {
    version_cell: ebr::AtomicArc<VersionCell<AtomicSequencer>>,
}

impl Default for RecordVersion {
    fn default() -> Self {
        RecordVersion {
            version_cell: ebr::AtomicArc::new(VersionCell::new()),
        }
    }
}
impl RecordVersion {
    pub fn new() -> RecordVersion {
        Default::default()
    }
}

impl Version<AtomicSequencer> for RecordVersion {
    type Data = RecordVersion;

    fn version_cell_ptr<'b>(
        &self,
        barrier: &'b ebr::Barrier,
    ) -> ebr::Ptr<'b, VersionCell<AtomicSequencer>> {
        self.version_cell.load(Relaxed, barrier)
    }

    fn create<'b>(
        &self,
        creator_ptr: ebr::Ptr<'b, crate::journal::Anchor<AtomicSequencer>>,
        barrier: &'b ebr::Barrier,
    ) -> Option<crate::VersionLocker<AtomicSequencer>> {
        if let Some(version_cell) = self.version_cell_ptr(barrier).try_into_arc() {
            return crate::VersionLocker::lock(version_cell, creator_ptr, barrier);
        }
        None
    }

    fn write(&mut self, _payload: RecordVersion) -> Option<Log> {
        None
    }

    fn read(
        &self,
        _snapshot: &Snapshot<AtomicSequencer>,
        _barrier: &ebr::Barrier,
    ) -> Option<&RecordVersion> {
        None
    }

    fn predate(&self, snapshot: &Snapshot<AtomicSequencer>, barrier: &ebr::Barrier) -> bool {
        if let Some(version_cell_ref) = self.version_cell_ptr(barrier).as_ref() {
            return version_cell_ref.predate(snapshot, barrier);
        }
        // The lack of `VersionCell` indicates that the object has been fully consolidated.
        true
    }

    fn consolidate(&self, barrier: &ebr::Barrier) -> bool {
        let version_cell_shared = self.version_cell.swap(Shared::null(), Relaxed, barrier);
        if version_cell_shared.is_null() {
            false
        } else {
            unsafe { barrier.defer_destroy(version_cell_shared) };
            true
        }
    }
}
