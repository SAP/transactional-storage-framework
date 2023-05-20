// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

//! The module defines the [`Sequencer`] trait.
//!
//! The [`Sequencer`] trait and the [`Instant`](Sequencer::Instant) are the basis of all the
//! database operations as they define the flow of time.

mod monotonic_u64;
pub use monotonic_u64::MonotonicU64;

use std::fmt::Debug;
use std::sync::atomic::Ordering;

/// [`Sequencer`] acts as a logical clock for the storage system.
///
/// A logical clock is the most important feature of a transactional storage system as it defines
/// the flow of time.
///
/// Developers are able to implement their own sequencing mechanism other than a simple atomic
/// counter by implementing the [`Sequencer`] trait, for instance, the system timestamp generator
/// can directly be used, or an efficient hardware-aided counter can also be incorporated.
pub trait Sequencer: 'static + Debug + Default + Send + Sync + Unpin {
    /// [`Instant`](Sequencer::Instant) is a partially ordered type representing an instant in a
    /// database system.
    ///
    /// It should satisfy [`Clone`], [`Copy`], [`PartialEq`], [`PartialOrd`], [`Send`], and
    /// [`Sync`].
    ///
    /// [`Clone`], [`Copy`], [`Send`] and [`Sync`] are required as the value can be copied and sent
    /// across threads and awaits frequently. [`PartialEq`] and [`PartialOrd`] allow developers to
    /// implement a floating-point, or a `Vector Clock` generator.
    ///
    /// The [`Default`] value is regarded as `⊥`, and the value is not allowed to be used by a
    /// transaction as its commit time point value.
    type Instant: Clone + Copy + Debug + Default + PartialEq + PartialOrd + Send + Sync + Unpin;

    /// [`Tracker`](Sequencer::Tracker) allows the sequencer to track every actively used
    /// [`Instant`](Sequencer::Instant) instance associated with a [`Snapshot`](super::Snapshot).
    ///
    /// A [`Tracker`](Sequencer::Tracker) can be cloned.
    type Tracker<'s>: Clone + Debug + ToInstant<Self>;

    /// Returns an [`Instant`](Sequencer::Instant) that represents a database snapshot being
    /// visible to all the current and future readers.
    fn min(&self, order: Ordering) -> Self::Instant;

    /// Gets the current [`Instant`](Sequencer::Instant).
    fn now(&self, order: Ordering) -> Self::Instant;

    /// Tracks the current [`Instant`](Sequencer::Instant) value by wrapping it in a
    /// [`Tracker`](Sequencer::Tracker).
    fn track(&self, order: Ordering) -> Self::Tracker<'_>;

    /// Updates the current logical [`Instant`](Sequencer::Instant) value.
    ///
    /// It tries to replace the current [`Instant`](Sequencer::Instant) value with the given one,
    /// and returns the most recent [`Instant`](Sequencer::Instant).
    ///
    /// # Errors
    ///
    /// It returns an error along with the latest [`Instant`](Sequencer::Instant) value if the
    /// supplied value was unusable for the [`Sequencer`], e.g., too old.
    fn update(
        &self,
        new_sequence: Self::Instant,
        order: Ordering,
    ) -> Result<Self::Instant, Self::Instant>;

    /// Advances its own [`Instant`](Sequencer::Instant).
    ///
    /// It returns the updated [`Instant`](Sequencer::Instant).
    fn advance(&self, order: Ordering) -> Self::Instant;
}

/// The [`ToInstant`] trait defines the capability of deriving an [`Instant`](Sequencer::Instant).
pub trait ToInstant<S: Sequencer> {
    /// Returns the corresponding [`Instant`](Sequencer::Instant) value.
    fn to_instant(&self) -> S::Instant;
}
