// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::{Metadata, Sequencer};

/// [`Container`] is a collection of organized data and its [`Metadata`].
#[derive(Debug)]
pub struct Container<S: Sequencer> {
    /// The metadata describing the specification of the [`Container`].
    _metadata: Metadata,

    /// A link to old versions of the [`Container`].
    _version: std::marker::PhantomData<S>,
}

impl<S: Sequencer> Container<S> {
    /// Creates a new data [`Container`].
    #[must_use]
    pub(super) fn new(metadata: Metadata) -> Container<S> {
        Container {
            _metadata: metadata,
            _version: std::marker::PhantomData,
        }
    }
}

#[cfg(test)]
mod test {
    use crate::sequencer::AtomicCounter;
    use crate::{Container, Metadata};

    #[tokio::test]
    async fn container() {
        let metadata = Metadata {};
        let _container = Container::<AtomicCounter>::new(metadata);
    }
}
