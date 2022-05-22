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
//! # [`DataPlane`]
//! [`DataPlane`] defines interfaces between [`Container`] and actual data.
//!
//! # [`Transaction`]
//! [`Transaction`] represents a storage transaction.

mod container;
pub use container::Container;

mod data_plane;
pub use data_plane::{DataPlane, RelationalTable};

mod error;
pub use error::Error;

mod journal;
pub use journal::Journal;

mod logger;
pub use logger::{FileLogger, Log, Logger};

mod record;
pub use record::Record;

mod layout;
pub use layout::Layout;

pub mod sequencer;
pub use sequencer::{AtomicCounter, Sequencer};

mod snapshot;
pub use snapshot::Snapshot;

mod storage;
pub use storage::Storage;

mod transaction;
pub use transaction::{Rubicon, Transaction};

pub mod version;
pub use version::{RecordVersion, Version};

mod tests;
