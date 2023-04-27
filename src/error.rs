// SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use std::io;

/// [`Error`] defines all the error codes used by the database storage system.
///
/// This only defines error codes used in the framework, and individual component implementations
/// may define separate error codes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Error {
    /// The operation conflicts with others.
    Conflict,

    /// The operation causes a deadlock.
    Deadlock,

    /// Generic errors with a message string attached to it.
    Generic(&'static str),

    /// IO error.
    IO(io::ErrorKind),

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
