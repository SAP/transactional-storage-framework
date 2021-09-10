// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

//! The module defines the [Sequencer] trait.
//!
//! The [Sequencer] trait and the [Clock](Sequencer::Clock) are the basis of all the database
//! operations as they define the flow of time.

use std::fmt::Debug;
use std::sync::atomic::Ordering;

/// [Sequencer] acts as a logical clock for the storage system.
///
/// A logical clock is the most important feature of a transactional storage system as it
/// defines the flow of time.
///
/// Developers are able to implement their own sequencing mechanism other than a simple atomic
/// counter by using the [Sequencer] trait, for instance, the system timestamp generator can
/// directly be used, or an efficient hardware-aided counter can also be incorporated.
pub trait Sequencer: 'static + Default {
    /// [Clock](Sequencer::Clock) is a partially ordered type representing a single point of
    /// time in a system.
    ///
    /// It should satisfy [Clone], [Copy], [`PartialEq`], [`PartialOrd`], [Send], and [Sync].
    ///
    /// [Clone], [Copy], [Send] and [Sync] are required as the value can be copied sent
    /// frequently. [`PartialEq`] and [`PartialOrd`] allow developers to implement a
    /// floating-point, or a `Lamport vector clock` generator.
    ///
    /// The [Default] value is treated an `invisible` time point in the system.
    type Clock: Clone + Copy + Debug + Default + PartialEq + PartialOrd + Send + Sync;

    /// [Tracker](Sequencer::Tracker) allows the sequencer to track all the issued
    /// [Clock](Sequencer::Clock) instances.
    ///
    /// A [Tracker](Sequencer::Tracker) can be cloned.
    type Tracker: Clone + DeriveClock<Self::Clock>;

    /// Returns a [Clock](Sequencer::Clock) that represents a database snapshot being visible
    /// to all the current and future readers.
    ///
    /// This must not return the default [Clock](Sequencer::Clock) value.
    fn min(&self, order: Ordering) -> Self::Clock;

    /// Gets the current [Clock](Sequencer::Clock).
    ///
    /// This must not return the default [Clock](Sequencer::Clock) value.
    fn get(&self, order: Ordering) -> Self::Clock;

    /// Issues a [Clock](Sequencer::Clock) wrapped in a [Tracker](Sequencer::Tracker).
    ///
    /// The [Sequencer] takes the issued [Clock](Sequencer::Clock) into account when
    /// calculating the minimum valid [Clock](Sequencer::Clock) value until the
    /// [Tracker](Sequencer::Tracker) is dropped.
    ///
    /// This must not issue the default [Clock](Sequencer::Clock) value.
    fn issue(&self, order: Ordering) -> Self::Tracker;

    /// Updates the current logical [Clock](Sequencer::Clock) value.
    ///
    /// It tries to replace the current [Clock](Sequencer::Clock) value with the given one. It
    /// returns the result of the update along with the latest value of the clock.
    ///
    /// # Errors
    ///
    /// It returns an error along with the latest [Clock](Sequencer::Clock) value of the
    /// [Sequencer] when the given value is unsuitable for the [Sequencer], for example, the
    /// supplied [Clock](Sequencer::Clock) is too old.
    fn update(
        &self,
        new_sequence: Self::Clock,
        order: Ordering,
    ) -> Result<Self::Clock, Self::Clock>;

    /// Advances its own [Clock](Sequencer::Clock).
    ///
    /// It returns the updated [Clock](Sequencer::Clock).
    fn advance(&self, order: Ordering) -> Self::Clock;
}

/// The [`DeriveClock`] trait defines the capability of deriving a [Clock](Sequencer::Clock).
pub trait DeriveClock<C> {
    /// Returns the [Clock](Sequencer::Clock).
    fn clock(&self) -> C;
}
