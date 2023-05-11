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
pub use access_controller::{AccessController};

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
pub use persistence_layer::{AwaitIO, FileIO, PersistenceLayer};

pub mod sequencer;
pub use sequencer::{MonotonicU64, Sequencer};

mod snapshot;
pub use snapshot::Snapshot;

mod transaction;
pub use transaction::ID as TransactionID;
pub use transaction::{Committable, Transaction};

pub mod utils;

mod task_processor;
mod telemetry;

#[cfg(test)]
mod tests;
