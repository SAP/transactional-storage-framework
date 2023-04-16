// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

//! Collection of utility functions.

use std::thread::{current, ThreadId};

/// Returns the current thread id.
#[inline]
#[must_use]
pub fn thread_id() -> ThreadId {
    THREAD_ID.with(|id| *id)
}

thread_local! {
    static THREAD_ID: ThreadId = current().id();
}
