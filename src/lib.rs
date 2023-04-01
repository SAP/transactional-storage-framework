// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

#![deny(missing_docs, warnings, clippy::all, clippy::pedantic)]

//! Transactional Storage Framework
//!
//! # [`Database`]
//! [`Database`] is a transactional storage system, and it is the gateway to all the functionalities that the crate offers.

mod access_controller;
pub use access_controller::AccessController;

mod container;
pub use container::Container;

mod database;
pub use database::Database;

mod error;
pub use error::Error;

mod journal;
pub use journal::Journal;

mod metadata;
pub use metadata::Metadata;

mod persistence_layer;
pub use persistence_layer::{PersistenceLayer, VolatileDevice};

pub mod sequencer;
pub use sequencer::{AtomicCounter, Sequencer};

mod snapshot;
pub use snapshot::Snapshot;

mod transaction;
pub use transaction::{Committable, Transaction};

mod overseer;

mod tests;
