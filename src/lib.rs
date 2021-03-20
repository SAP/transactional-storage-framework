// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

//! Transactional Storage Framework
//!
//! # [`tss::Storage`]
//! [`tss::Storage`] is a transactional storage system, and it is the gateway to all the functionalities that the crate offers.
//! # [`tss::Container`]
//! [`tss::Container`] is a hierachical data container that [`tss::Storage`] manages.
//! # [`tss::Transaction`]
//! [`tss::Transaction`] represents storage transactions.
//!
//! [`tss::Storage`]: storage::Storage
//! [`tss::Container`]: container::Container
//! [`tss::Transaction`]: transaction::Transaction

// Public traits.
mod container;
mod error;
mod logger;
mod sequencer;
mod snapshot;
mod storage;
mod transaction;
mod version;
pub use container::{Container, ContainerData, ContainerHandle};
pub use error::Error;
pub use logger::{Log, Logger};
pub use sequencer::{DeriveClock, Sequencer};
pub use snapshot::Snapshot;
pub use storage::Storage;
pub use transaction::{Journal, JournalAnchor, Transaction};
pub use version::{DefaultVersionedObject, Version, VersionCell, VersionLocker};

// Implementation.
mod r#impl;
pub use r#impl::atomic_counter::AtomicCounter;
pub use r#impl::file_logger::FileLogger;
pub use r#impl::relational_table::RelationalTable;
