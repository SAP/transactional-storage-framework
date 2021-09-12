// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use std::any::TypeId;

/// [Layout] defines the layout of a single [Record](super::Record).
pub trait Layout {
    /// The number of fields in a [Record](super::Record).
    const WIDTH: usize;

    /// Returns the [`TypeId`] of the specified field.
    fn field_type(pos: usize) -> Option<TypeId>;
}
