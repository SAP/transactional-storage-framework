// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::unused_async, unused)]

use super::{Metadata, Sequencer};

/// [`Container`] is a collection of organized data and its [`Metadata`].
#[derive(Debug)]
pub struct Container<S: Sequencer> {
    /// The metadata describing the specification of the [`Container`].
    metadata: Metadata,

    /// Unimplemented.
    _phantom: std::marker::PhantomData<S>,
}

impl<S: Sequencer> Container<S> {
    /// Creates a new data [`Container`].
    #[must_use]
    pub(super) fn new(metadata: Metadata) -> Container<S> {
        Container {
            metadata,
            _phantom: std::marker::PhantomData,
        }
    }
}

#[cfg(test)]
mod test {
    use crate::sequencer::AtomicCounter;
    use crate::Database;

    use std::sync::Arc;

    #[tokio::test]
    async fn insert_delete_vacuum() {
        let storage: Arc<Database<AtomicCounter>> = Arc::new(Database::default());

        let transaction = storage.transaction();
        let snapshot = transaction.snapshot();
        let journal = transaction.start();
        drop(journal);
        drop(snapshot);
        drop(transaction);
    }
}
