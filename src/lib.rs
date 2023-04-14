// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

#![deny(
    missing_docs,
    warnings,
    clippy::all,
    clippy::pedantic,
    clippy::undocumented_unsafe_blocks
)]

//! SAP Transactional Storage Framework

mod access_controller;
pub use access_controller::{AccessController, ToObjectID};

mod container;
pub use container::Container;

mod database;
pub use database::Database;

mod error;
pub use error::Error;

mod journal;
pub use journal::Journal;
pub use journal::ID as JournalID;

mod metadata;
pub use metadata::Metadata;

mod persistence_layer;
pub use persistence_layer::{AwaitIO, PersistenceLayer, VolatileDevice};

pub mod sequencer;
pub use sequencer::{AtomicCounter, Sequencer};

mod snapshot;
pub use snapshot::Snapshot;

mod transaction;
pub use transaction::ID as TransactionID;
pub use transaction::{Committable, Transaction};

mod overseer;

mod tests;
