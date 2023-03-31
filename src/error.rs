// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

/// [`Error`] defines all the error codes used by the database system.
pub enum Error {
    /// The operation conflicts with others.
    Conflict,

    /// The desired resource could not be found in the database.
    NotFound,

    /// The operation failed to be serialized with others.
    SerializationFailure,

    /// The operation encountered the target database object being in an unexpected state.
    UnexpectedState,

    /// The operation causes the same key to be inserted into a unique container.
    UniquenessViolation,
}
