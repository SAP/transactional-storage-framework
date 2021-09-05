// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::journal::Anchor as JournalAnchor;
use super::{Log, Sequencer, Snapshot};

use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

use scc::ebr;

/// The [Version] trait stipulates interfaces of versioned database objects.
///
/// All the versioned database objects in a [Storage](super::Storage) must implement the trait.
pub trait Version<S: Sequencer> {
    /// The type of the versioned data.
    type Data: Send + Sync;

    /// Returns an [ebr::Ptr] to the [VersionCell] to which the versioned database object
    /// corresponds.
    fn version_cell_ptr<'b>(&self, barrier: &'b ebr::Barrier) -> ebr::Ptr<'b, VersionCell<S>>;

    /// Creates a new [Version] of the versioned database object.
    ///
    /// A versioned database object becomes reachable before its contents are fully
    /// materialized, and a transaction that successfully calls this method is eligible to fill
    /// it with contents. The contents are globally visible right after the transaction is
    /// committed.
    fn create<'b>(
        &self,
        creator_ptr: ebr::Ptr<'b, JournalAnchor<S>>,
        barrier: &'b ebr::Barrier,
    ) -> Option<VersionLocker<S>> {
        if let Some(version_cell) = self.version_cell_ptr(barrier).try_into_arc() {
            return VersionLocker::lock(version_cell, creator_ptr, barrier);
        }
        None
    }

    /// The creator of the [Version] is eligible to feed data.
    ///
    /// The caller must own the [Version], or a [VersionLocker] that owns it.
    fn write(&mut self, payload: Self::Data) -> Option<Log>;

    /// Returns a reference to the data.
    ///
    /// It does not return a reference if the [Snapshot] predates the versioned object.
    fn read(&self, snapshot: &Snapshot<S>, barrier: &ebr::Barrier) -> Option<&Self::Data>;

    /// Returns `true` if the [Version] predates the [Snapshot].
    fn predate(&self, snapshot: &Snapshot<S>, barrier: &ebr::Barrier) -> bool {
        if let Some(version_cell_ref) = self.version_cell_ptr(barrier).as_ref() {
            return version_cell_ref.predate(snapshot, barrier);
        }
        // The lack of `VersionCell` indicates that the object has been fully consolidated.
        true
    }

    /// Consolidates the versioned database object to make it globally visible.
    ///
    /// Returns `true` if it has successfully detached the versioning information.
    fn consolidate(&self, barrier: &ebr::Barrier) -> bool;
}

/// [VersionCell] is a piece of data that is associated with a versioned database object.
///
/// [VersionCell] store the owner of the [Version] and the time point when the version is
/// created.
pub struct VersionCell<S: Sequencer> {
    /// The current owner of the [VersionCell].
    ///
    /// Readers have to check the transaction state when it points to a [JournalAnchor].
    owner: ebr::AtomicArc<JournalAnchor<S>>,

    /// Represents a point of time when the [Version] is created or deleted.
    ///
    /// The time point value cannot be reset, or updated once set by a transaction.
    time_point: S::Clock,
}

impl<S: Sequencer> VersionCell<S> {
    /// Checks if the [VersionCell] predates the snapshot.
    fn predate(&self, snapshot: &Snapshot<S>, barrier: &ebr::Barrier) -> bool {
        if self.time_point != S::Clock::default() {
            return self.time_point <= *snapshot.clock();
        }

        // Checks the owner.
        //
        // It has to be a load-acquire in order to read `self.time_point` correctly.
        // Synchronization among transactions and readers through the `Sequencer` is
        // insufficient, because `VersionCell` is asynchronously updated after a new logical
        // clock is generated for a transaction.
        if let Some(owner_ref) = self.owner.load(Acquire, barrier).as_ref() {
            if snapshot.visible(owner_ref, &barrier) {
                // The change has been made by the transaction that predates the snapshot.
                return true;
            }
        }

        // Checks the time point again.
        self.time_point != S::Clock::default() && self.time_point <= *snapshot.clock()
    }
}

impl<S: Sequencer> Default for VersionCell<S> {
    fn default() -> Self {
        Self {
            owner: ebr::AtomicArc::default(),
            time_point: S::Clock::default(),
        }
    }
}

impl<S: Sequencer> Drop for VersionCell<S> {
    fn drop(&mut self) {
        // Not locked when it is dropped.
        debug_assert!(self.owner.is_null(Relaxed));
    }
}

/// [VersionLocker] owns a [VersionCell] instance by holding a strong reference to it.
pub struct VersionLocker<S: Sequencer> {
    /// [VersionCell] holds a strong reference to [VersionCell].
    version_cell: ebr::Arc<VersionCell<S>>,

    /// The current owner of the [VersionLocker].
    current_owner: *const JournalAnchor<S>,

    /// The previous owner.
    ///
    /// There are cases where ownership is transferred from a [Journal](super::Journal) to
    /// another; they belong to the same transaction, and this one predates the one trying to
    /// acquire the lock.
    prev_owner: Option<ebr::Arc<JournalAnchor<S>>>,
}

impl<S: Sequencer> VersionLocker<S> {
    /// Acquires the exclusive lock on the given [VersionCell].
    ///
    /// If the [VersionCell] has a valid time point assigned, it returns `None`.
    fn lock(
        version_cell: ebr::Arc<VersionCell<S>>,
        new_owner_ptr: ebr::Ptr<JournalAnchor<S>>,
        barrier: &ebr::Barrier,
    ) -> Option<VersionLocker<S>> {
        if version_cell.time_point != S::Clock::default() {
            // The `VersionCell` has been created by another transaction.
            return None;
        }

        let mut new_owner = new_owner_ptr.try_into_arc();
        if new_owner.is_none() {
            // The given pointer is invalid.
            return None;
        }

        while let Err((passed, actual)) = version_cell.owner.compare_exchange(
            ebr::Ptr::null(),
            (new_owner_ptr.try_into_arc(), ebr::Tag::None),
            Relaxed,
            Relaxed,
        ) {
            new_owner = passed;
            if new_owner.is_none() {
                // The given pointer is invalid.
                return None;
            }

            if new_owner_ptr == actual {
                // The `Journal` has previously acquired the lock.
                debug_assert_eq!(version_cell.time_point, S::Clock::default());
                return Some(VersionLocker {
                    version_cell,
                    current_owner: new_owner_ptr.as_raw(),
                    prev_owner: new_owner,
                });
            }

            // There is no way the actual pointer was null.
            let actual_owner = actual.as_ref().unwrap();

            let (same_trans, lockable) =
                new_owner.as_ref().unwrap().lockable(actual_owner, barrier);

            if same_trans {
                if !lockable {
                    // The versioned object is locked by an active `Journal` in the same
                    // transaction.
                    return None;
                }
                // Tries to take ownership.
                match version_cell.owner.compare_exchange(
                    actual,
                    (new_owner, ebr::Tag::None),
                    Acquire,
                    Relaxed,
                ) {
                    Err((passed, _)) => {
                        new_owner = passed;
                        continue;
                    }
                    Ok((old, _)) => {
                        // Succesfully took ownership.
                        debug_assert_eq!(version_cell.time_point, S::Clock::default());
                        return Some(VersionLocker {
                            version_cell,
                            current_owner: new_owner_ptr.as_raw(),
                            prev_owner: old,
                        });
                    }
                }
            }

            // Waits for the actual owner to release the lock.
            if actual_owner
                .wait(
                    |snapshot| {
                        if snapshot == S::Clock::default() {
                            // The transaction has been rolled back, or the `Journal` has been
                            // discarded, it will try to overtake ownership.
                            //
                            // The following CAS returning `false` means that another
                            // transaction overtook ownership.
                            if let Err((passed, _)) = version_cell.owner.compare_exchange(
                                actual,
                                (new_owner, ebr::Tag::None),
                                Acquire,
                                Relaxed,
                            ) {
                                new_owner = passed;
                                return false;
                            }
                            return true;
                        }
                        false
                    },
                    barrier,
                )
                .map_or_else(|| false, |result| result)
            {
                // This transaction has successfully locked the `VersionCell`.
                break;
            }

            if version_cell.time_point != S::Clock::default() {
                // The `VersionCell` has a valid time point.
                return None;
            }
        }

        if version_cell.time_point != S::Clock::default() {
            // The `VersionCell` has a valid time point.
            version_cell.owner.swap((None, ebr::Tag::None), Relaxed);
            return None;
        }

        Some(VersionLocker {
            version_cell,
            current_owner: new_owner_ptr.as_raw(),
            prev_owner: None,
        })
    }

    /// Converts the given [Version] reference into a mutable reference, and updates it.
    pub fn write<V: Version<S>>(
        &self,
        version: &V,
        payload: V::Data,
        barrier: &ebr::Barrier,
    ) -> Result<Option<Log>, ()> {
        if self.version_cell.ptr(barrier) == version.version_cell_ptr(barrier) {
            let version_mut_ref = unsafe { &mut *(version as *const _ as *mut V) };
            return Ok(version_mut_ref.write(payload));
        }
        Err(())
    }
}

impl<S: Sequencer> Drop for VersionLocker<S> {
    fn drop(&mut self) {
        let barrier = ebr::Barrier::new();
        *unsafe { &mut *(&self.version_cell.time_point as *const S::Clock as *mut S::Clock) } =
            self.version_cell
                .owner
                .load(Relaxed, &barrier)
                .as_ref()
                .unwrap()
                .commit_snapshot();
        let mut current_owner = self.version_cell.owner.load(Relaxed, &barrier);
        while current_owner.as_raw() == self.current_owner {
            // It must be a release-store.
            if let Err((_, actual)) = self.version_cell.owner.compare_exchange(
                current_owner,
                (self.prev_owner.take(), ebr::Tag::None),
                Release,
                Relaxed,
            ) {
                current_owner = actual;
            } else {
                break;
            }
        }
    }
}
