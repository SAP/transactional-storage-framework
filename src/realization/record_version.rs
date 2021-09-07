// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use crate::version::Owner;
use crate::{AtomicCounter, Version};

/// [`RecordVersion`] is a versioned database object for a record.
#[derive(Default)]
pub struct RecordVersion<D: Default + Send + Sync> {
    owner: Owner<AtomicCounter>,
    data: D,
}

impl<D: Default + Send + Sync> Version<AtomicCounter> for RecordVersion<D> {
    type Data = D;

    fn owner_field<'b>(&self) -> &Owner<AtomicCounter> {
        &self.owner
    }

    fn data_ref(&self) -> &Self::Data {
        &self.data
    }
}
