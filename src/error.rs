// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

/// [`Error`] defines all the error codes used by the database system.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Error {
    /// The operation conflicts with others.
    Conflict,

    /// The operation causes a deadlock.
    Deadlock,

    /// The desired resource could not be found in the database.
    NotFound,

    /// Memory allocation failed.
    OutOfMemory,

    /// The operation failed to be serialized with others.
    SerializationFailure,

    /// The operation was timed out.
    Timeout,

    /// The operation encountered the target database object being in an unexpected state.
    UnexpectedState,

    /// The operation causes the same key to be inserted into a unique container.
    UniquenessViolation,

    /// The supplied parameter value is wrong.
    WrongParameter,
}
