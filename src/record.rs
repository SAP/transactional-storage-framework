// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use super::{Sequencer, Layout};

/// [Record] is a single record in a [`DataPlane`](super::DataPlane).
pub trait Record<S: Sequencer, L: Layout> {
}
