// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

//! This module defines data types used for versioning database objects.
//!
//! The data types in the module rely on the [transmute] operator to prolong the lifetime of
//! their instances, because there is no other way that the code is able to tell the compiler
//! that they will be safely garbage-collected in accordance with a correct database `MVCC`
//! mechanism.

use super::journal::Anchor as JournalAnchor;
use super::{Error, Log, Sequencer, Snapshot};

use std::mem::transmute;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::time::Duration;

use scc::ebr;

/// The [Version] trait stipulates interfaces of versioned database objects.
///
/// All the versioned database objects in a [Storage](super::Storage) must implement the trait.
pub trait Version<S: Sequencer> {
    /// The type of the versioned data.
    type Data: Send + Sync;

    /// Returns an [Owner] reference that points to the creator transaction.
    ///
    /// It is allowed for a [Version] implementation to return different references based on
    /// the status, for instance, if the [Version] is fully consolidated, it may return a
    /// static reference to an [Owner] instance that represents an always-visible state.
    ///
    /// # Safety
    ///
    /// The owner field must not be dropped on its own, otherwise a [Locker] may gain access to
    /// freed memory. [Version] is meant to be garbage-collected by a correctly implemented
    /// garbage collector in its associated [Storage](super::Storage).
    fn owner_field(&self) -> &Owner<S>;

    /// Returns its data.
    ///
    /// It indiscriminately returns a reference to its data. Reading the data without checking
    /// visibility using the `predate` method is deemed unsafe.
    fn data_ref(&self) -> &Self::Data;

    /// Creates a new [Version] of the versioned database object.
    ///
    /// An uninitialized versioned database object becomes reachable before its contents are
    /// fully materialized, and a transaction that successfully calls this method is eligible
    /// to fill it with contents. The contents are globally visible right after the transaction
    /// is committed.
    fn create<'b>(
        &self,
        creator_ptr: ebr::Ptr<'b, JournalAnchor<S>>,
        timeout: Option<Duration>,
        barrier: &'b ebr::Barrier,
    ) -> Option<Locker<S>> {
        Locker::lock(self.owner_field(), creator_ptr, timeout, barrier)
    }

    /// The creator of the [Version] is mandated to feed data.
    ///
    /// The caller must exclusively own a [Locker] that owns the [Version].
    ///
    /// # Errors
    ///
    /// It returns an error if the given [Locker] does not own `self` or the writer returns an
    /// error.
    fn write<F: FnOnce(&mut Self::Data) -> Result<Option<Log>, Error>>(
        &self,
        locker: &mut Locker<S>,
        writer: F,
        barrier: &ebr::Barrier,
    ) -> Result<Option<Log>, Error> {
        if self.owner_field().0.load(Relaxed, barrier)
            == locker.owner_field.0.load(Relaxed, barrier)
        {
            #[allow(clippy::cast_ref_to_mut)]
            let data_mut_ref =
                unsafe { &mut *(self.data_ref() as *const Self::Data as *mut Self::Data) };
            let log = writer(data_mut_ref)?;
            return Ok(log);
        }
        Err(Error::Fail)
    }

    /// Returns `true` if the [Version] predates the [Snapshot].
    fn predate(&self, snapshot: &Snapshot<S>, barrier: &ebr::Barrier) -> bool {
        let owner_ptr = self.owner_field().0.load(Relaxed, barrier);
        if let Some(journal_anchor_ref) = owner_ptr.as_ref() {
            return snapshot.visible(journal_anchor_ref, barrier);
        }
        // The lack of `JournalAnchor` indicates that the object has been fully consolidated.
        owner_ptr.tag() == ebr::Tag::First
    }

    /// Consolidates the versioned database object to make it globally visible.
    ///
    /// Returns `true` if it has successfully detached the versioning information. If it is
    /// called on an uninitialized [Version], the [Version] remains uninitialized permanently.
    fn consolidate(&self) -> bool {
        self.owner_field()
            .0
            .swap((None, ebr::Tag::First), Relaxed)
            .is_some()
    }
}

/// [Owner] is a mandatory field in a [Version] in order for the [Version] to be correctly
/// locked and updated.
#[derive(Default)]
pub struct Owner<S: Sequencer>(ebr::AtomicArc<JournalAnchor<S>>);

impl<S: Sequencer> Owner<S> {
    /// Returns the snapshot clock value at the commit time of the owner.
    ///
    /// If it is not owned, the default value is returned.
    fn commit_clock(&self, barrier: &ebr::Barrier) -> S::Clock {
        self.0
            .load(Relaxed, barrier)
            .as_ref()
            .map_or_else(S::Clock::default, JournalAnchor::commit_snapshot)
    }
}

impl<S: Sequencer> Drop for Owner<S> {
    fn drop(&mut self) {
        // This must not spin if the lifetime of its associated `Version` is properly managed.
        while !self.0.is_null(Relaxed) {
            let barrier = ebr::Barrier::new();
            let owner_ptr = self.0.load(Acquire, &barrier);
            if let Some(owner_ref) = owner_ptr.as_ref() {
                if owner_ref.commit_snapshot() != S::Clock::default() {
                    // The transaction has been committed, and there is no possibility of a
                    // `Locker` trying to release it.
                    break;
                }
            }
        }
    }
}

/// [Locker] has `'static` references to the [Version] and [Journal](super::Journal).
///
/// It semantically owns the [Version] while the `Rust` compiler cannot deduce anything related
/// to a database `MVCC` mechanism, and therefore instantiating a [Locker] requires calls to
/// [transmute](std::mem::transmute) to prolong lifetimes of references.
pub struct Locker<S: Sequencer> {
    /// [Locker] has a reference to the version owner field of the [Version].
    owner_field: &'static Owner<S>,

    /// The current owner of the [Locker].
    current_owner: ebr::Ptr<'static, JournalAnchor<S>>,

    /// The previous owner.
    ///
    /// There are cases where ownership is transferred from a [Journal](super::Journal) to
    /// another; they belong to the same transaction, and one predates the other one trying to
    /// acquire the lock.
    prev_owner: ebr::Ptr<'static, JournalAnchor<S>>,
}

impl<S: Sequencer> Locker<S> {
    /// Acquires the exclusive lock on the given [Version].
    ///
    /// If the [Version] has a valid time point assigned, it returns `None`.
    #[allow(clippy::too_many_lines)]
    fn lock<'b>(
        owner_field: &Owner<S>,
        new_owner_ptr: ebr::Ptr<JournalAnchor<S>>,
        timeout: Option<Duration>,
        barrier: &'b ebr::Barrier,
    ) -> Option<Locker<S>> {
        if owner_field.commit_clock(barrier) != S::Clock::default() {
            // The `Version` has been created by another transaction.
            return None;
        }

        let mut new_owner = new_owner_ptr.try_into_arc();
        new_owner.as_ref()?;

        while let Err((passed, actual)) = owner_field.0.compare_exchange(
            ebr::Ptr::null(),
            (new_owner.take(), ebr::Tag::None),
            Relaxed,
            Relaxed,
        ) {
            new_owner = passed;
            new_owner.as_ref()?;

            if actual.tag() == ebr::Tag::First {
                // The `Version` has been consolidated.
                return None;
            }

            if new_owner_ptr == actual {
                // The `Journal` has previously acquired the lock.
                debug_assert_eq!(owner_field.commit_clock(barrier), S::Clock::default());
                unsafe {
                    return Some(Locker {
                        owner_field: transmute(owner_field),
                        current_owner: transmute(new_owner_ptr),
                        prev_owner: transmute(new_owner_ptr),
                    });
                }
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
                match owner_field.0.compare_exchange(
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
                        // Successfully took ownership.
                        debug_assert_eq!(owner_field.commit_clock(barrier), S::Clock::default());
                        unsafe {
                            return Some(Locker {
                                owner_field: transmute(owner_field),
                                current_owner: transmute(new_owner_ptr),
                                prev_owner: old
                                    .map_or_else(ebr::Ptr::null, |a| transmute(a.ptr(barrier))),
                            });
                        }
                    }
                }
            }

            // Waits for the actual owner to release the lock.
            #[allow(clippy::blocks_in_if_conditions)]
            if actual_owner
                .wait(
                    |snapshot| {
                        if snapshot == S::Clock::default() {
                            // The transaction has been rolled back, or the `Journal` has been
                            // discarded, it will try to overtake ownership.
                            //
                            // The following CAS returning `false` means that another
                            // transaction overtook ownership.
                            if let Err((passed, _)) = owner_field.0.compare_exchange(
                                actual,
                                (new_owner.take(), ebr::Tag::None),
                                Acquire,
                                Relaxed,
                            ) {
                                // TODO: retry if `actual` and the new `current` are in the same transaction.
                                if let Some(passed) = passed {
                                    new_owner.replace(passed);
                                }
                                return false;
                            }
                            return true;
                        }
                        false
                    },
                    timeout,
                )
                .map_or_else(|| false, |result| result)
            {
                // This transaction has successfully locked the `VersionCell`.
                break;
            }

            if owner_field.commit_clock(barrier) != S::Clock::default() {
                // The `VersionCell` has a valid time point.
                return None;
            }

            if timeout.is_some() {
                // The time-out reached.
                return None;
            }
        }

        let locker = unsafe {
            Locker {
                owner_field: transmute(owner_field),
                current_owner: transmute(new_owner_ptr),
                prev_owner: ebr::Ptr::null(),
            }
        };

        if locker.owner_field.commit_clock(barrier) != S::Clock::default() {
            // The `VersionCell` has a valid time point.
            drop(locker);
            return None;
        }

        Some(locker)
    }
}

impl<S: Sequencer> Drop for Locker<S> {
    fn drop(&mut self) {
        // This `Locker` reverts the state of the `Version` if the transaction has not been
        // committed.
        if self
            .current_owner
            .as_ref()
            .map_or_else(|| false, |j| j.commit_snapshot() == S::Clock::default())
        {
            let barrier = ebr::Barrier::new();
            let mut current_owner = self.owner_field.0.load(Relaxed, &barrier);
            while current_owner == self.current_owner {
                // It must be a release-store.
                if let Err((_, actual)) = self.owner_field.0.compare_exchange(
                    current_owner,
                    (self.prev_owner.try_into_arc(), ebr::Tag::None),
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
}

/// The [`ebr::Ptr`] instances in a [`Locker`] can be sent as they are not associated with an
/// [`ebr::Barrier`].
unsafe impl<S: Sequencer> Send for Locker<S> {}
