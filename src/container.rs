// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::{Metadata, PersistenceLayer, Sequencer};

/// [`Container`] is a collection of organized data and its [`Metadata`].
#[derive(Debug)]
pub struct Container<S: Sequencer, P: PersistenceLayer<S>> {
    /// The metadata describing the specification of the [`Container`].
    _metadata: Metadata,

    /// A link to old versions of the [`Container`].
    _version: std::marker::PhantomData<(S, P)>,
}

impl<S: Sequencer, P: PersistenceLayer<S>> Container<S, P> {
    /// Creates a new data [`Container`].
    #[must_use]
    pub(super) fn new(metadata: Metadata) -> Container<S, P> {
        Container {
            _metadata: metadata,
            _version: std::marker::PhantomData,
        }
    }
}

#[cfg(test)]
mod test {
    use crate::sequencer::AtomicCounter;
    use crate::{Container, Metadata, VolatileDevice};

    #[tokio::test]
    async fn container() {
        let metadata = Metadata {};
        let _container = Container::<AtomicCounter, VolatileDevice<AtomicCounter>>::new(metadata);
    }
}
