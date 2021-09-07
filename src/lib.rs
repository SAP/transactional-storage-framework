// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

#![deny(missing_docs, warnings, clippy::all, clippy::pedantic)]

//! Transactional Storage Framework
//!
//! # [`Storage`]
//! [`Storage`] is a transactional storage system, and it is the gateway to all the functionalities that the crate offers.
//!
//! # [`Container`]
//! [`Container`] is hierarchical data container that a [`Storage`] manages.
//!
//! # [`Transaction`]
//! [`Transaction`] represents a storage transaction.

mod container;
pub use container::Container;

mod error;
pub use error::Error;

mod journal;
pub use journal::Journal;

mod logger;
pub use logger::{Log, Logger};

pub mod sequencer;
pub use sequencer::Sequencer;

mod snapshot;
pub use snapshot::Snapshot;

mod storage;
pub use storage::Storage;

mod transaction;
pub use transaction::{Rubicon, Transaction};

pub mod version;
pub use version::Version;

mod realization;
pub use realization::atomic_counter::AtomicCounter;
pub use realization::file_logger::FileLogger;
pub use realization::record_version::RecordVersion;
pub use realization::relational_table::RelationalTable;

mod tests;
