// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use scc::HashMap;

/// [`LockTable`] manages the state of individual records.
#[derive(Debug, Default)]
pub struct LockTable {
    _table: HashMap<u64, Entry>,
}

#[derive(Debug)]
struct Entry {}
