// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use crate::{JournalID, Sequencer, TransactionID};
use std::mem::{size_of, MaybeUninit};
use std::ptr::addr_of;

/// Individual log record representation.
///
/// The maximum size of a log record is limited to 32-byte when the size of `S::Instant` is not
/// larger than 24-byte.
///
/// The bit representation of [`LogRecord`] is as follows.
/// - 61-bit transaction ID, and 3-bit transaction control opcode follows.
/// - If `transaction opcode = 0b000`, the event is unrelated to a transaction.
/// -- `0b00000000`: the end of log file.
/// -- `0b00001000`: the buffer was submitted, and a `u32` value follows.
/// -- `0b00010000`: the buffer was discarded.
/// -- TODO: page reorganization.
/// - If `transaction opcode = 0b100`, the event happened in a transaction.
/// -- 61-bit journal ID, 3-bit opcode.
/// -- If `opcode = 0b000`, the journal created data identified as the `u64` value that follows.
/// -- If `opcode = 0b001`, the journal created data identified as the two `u64` values that
/// follow.
/// -- If `opcode = 0b010`, the journal deleted data identified as the `u64` value that follows.
/// -- If `opcode = 0b011`, the journal deleted data identified as the two `u64` values that
/// follow.
/// -- If `opcode = 0b100`, the journal was submitted.
/// -- If `opcode = 0b101`, the journal was discarded.
/// - If `transaction opcode = 0b101`, the transaction is being prepared for commit, and
/// `S::Instant` follows.
/// - If `transaction opcode = 0b110`, the transaction is being committed, and `S::Instant`
/// follows.
/// - If `transaction opcode = 0b111`, the transaction is being rolled back.
#[derive(Copy, Clone, Debug, Eq)]
pub(super) enum LogRecord<S: Sequencer> {
    /// End-of-log marker.
    ///
    /// Consecutive eight `0`s represent the end of a log file.
    EndOfLog,

    /// The log buffer was submitted.
    ///
    /// Log records of this type are not written to log buffers directly, instead they are derived
    /// from the log buffer state.
    BufferSubmitted(u32),

    /// The log buffer was discarded.
    ///
    /// Log records of this type are not written to log buffers directly, instead they are derived
    /// from the log buffer state.
    BufferDiscarded,

    /// The transaction created a single database object identified as the `u64` value.
    JournalCreatedObjectSingle(TransactionID, JournalID, u64),

    /// The transaction created a range of database objects identified as
    /// `(starting database object identifier, interval, number of objects)`.
    JournalCreatedObjectRange(TransactionID, JournalID, u64, u32, u32),

    /// The transaction deleted a single database object identified as the `u64` value.
    JournalDeletedObjectSingle(TransactionID, JournalID, u64),

    /// The transaction deleted a range of database objects identified as
    /// `(starting database object identifier, interval, number of objects)`.
    JournalDeletedObjectRange(TransactionID, JournalID, u64, u32, u32),

    /// The journal was submitted.
    ///
    /// The handling of this log record type is identical to that of [`Self::BufferSubmitted`].
    JournalSubmitted(TransactionID, JournalID, u32),

    /// The journal was discarded.
    ///
    /// The handling of this log record type is identical to that of [`Self::BufferDiscarded`].
    JournalDiscarded(TransactionID, JournalID),

    /// The transaction is prepared.
    TransactionPrepared(TransactionID, S::Instant),

    /// The transaction is committed.
    TransactionCommitted(TransactionID, S::Instant),

    /// The transaction is rolled back.
    TransactionRolledBack(TransactionID, u32),
}

/// Opcode bit mask.
pub const OPCODE_MASK: u64 = 0b111;

/// Extended opcode bit mask.
pub const EXTENDED_OPCODE_MASK: u64 = 0b1111_1000;

/// The log buffer was committed.
pub const BUFFER_COMMITTED: u64 = 0b0000_1000;

/// The log buffer was rolled back.
pub const BUFFER_ROLLED_BACK: u64 = 0b0001_0000;

/// The journal created a single database object.
pub const JOURNAL_CREATED_SINGLE: u64 = 0b000;

/// The journal created a range of database objects.
pub const JOURNAL_CREATED_RANGE: u64 = 0b001;

/// The journal deleted a single database object.
pub const JOURNAL_DELETED_SINGLE: u64 = 0b010;

/// The journal deleted a range of database objects.
pub const JOURNAL_DELETED_RANGE: u64 = 0b011;

/// The journal was submitted to the transaction.
pub const JOURNAL_SUBMITTED: u64 = 0b100;

/// The journal was discarded.
pub const JOURNAL_DISCARDED: u64 = 0b101;

/// The transaction updated the database.
pub const TRANSACTION_UPDATED: u64 = 0b100;

/// The transaction was prepared for commit.
pub const TRANSACTION_PREPARED: u64 = 0b101;

/// The transaction was committed.
pub const TRANSACTION_COMMITTED: u64 = 0b110;

/// The transaction was rolled back.
pub const TRANSACTION_ROLLED_BACK: u64 = 0b111;

impl<S: Sequencer> LogRecord<S> {
    /// Tries to parse the supplied `u8` slice as a [`LogRecord`].
    #[allow(clippy::too_many_lines)]
    pub(super) fn from_raw_data(value: &[u8]) -> Option<(Self, &[u8])> {
        let (transaction_id_with_opcode, value) = read_part::<TransactionID>(value)?;

        if transaction_id_with_opcode == 0 {
            return Some((Self::EndOfLog, value));
        }

        let transaction_opcode = transaction_id_with_opcode & OPCODE_MASK;
        if transaction_opcode == 0 {
            let extended_opcode = transaction_id_with_opcode & EXTENDED_OPCODE_MASK;
            match extended_opcode {
                BUFFER_COMMITTED => {
                    let transaction_instant: u32 = (transaction_id_with_opcode >> 32)
                        .try_into()
                        .ok()
                        .map_or(0, |v| v);
                    return Some((LogRecord::BufferSubmitted(transaction_instant), value));
                }
                BUFFER_ROLLED_BACK => {
                    return Some((LogRecord::BufferDiscarded, value));
                }
                _ => unimplemented!(),
            }
        }

        let transaction_id = transaction_id_with_opcode & (!OPCODE_MASK);
        match transaction_opcode {
            TRANSACTION_UPDATED => {
                let (journal_id_with_opcode, value) = read_part::<JournalID>(value)?;
                let journal_id = journal_id_with_opcode & (!OPCODE_MASK);
                let opcode = journal_id_with_opcode & OPCODE_MASK;
                match opcode {
                    JOURNAL_CREATED_SINGLE => {
                        // Created.
                        let (object_id, value) = read_part::<u64>(value)?;
                        Some((
                            LogRecord::JournalCreatedObjectSingle(
                                transaction_id,
                                journal_id,
                                object_id,
                            ),
                            value,
                        ))
                    }
                    JOURNAL_CREATED_RANGE => {
                        // Created range.
                        let (object_id, value) = read_part::<u64>(value)?;
                        let (interval, value) = read_part::<u32>(value)?;
                        let (num_objects, value) = read_part::<u32>(value)?;
                        Some((
                            LogRecord::JournalCreatedObjectRange(
                                transaction_id,
                                journal_id,
                                object_id,
                                interval,
                                num_objects,
                            ),
                            value,
                        ))
                    }
                    JOURNAL_DELETED_SINGLE => {
                        // Deleted.
                        let (object_id, value) = read_part::<u64>(value)?;
                        Some((
                            LogRecord::JournalDeletedObjectSingle(
                                transaction_id,
                                journal_id,
                                object_id,
                            ),
                            value,
                        ))
                    }
                    JOURNAL_DELETED_RANGE => {
                        // Deleted range.
                        let (object_id, value) = read_part::<u64>(value)?;
                        let (interval, value) = read_part::<u32>(value)?;
                        let (num_objects, value) = read_part::<u32>(value)?;
                        Some((
                            LogRecord::JournalDeletedObjectRange(
                                transaction_id,
                                journal_id,
                                object_id,
                                interval,
                                num_objects,
                            ),
                            value,
                        ))
                    }
                    JOURNAL_SUBMITTED => {
                        let (transaction_instant, value) = read_part::<u32>(value)?;
                        Some((
                            LogRecord::JournalSubmitted(
                                transaction_id,
                                journal_id,
                                transaction_instant,
                            ),
                            value,
                        ))
                    }
                    JOURNAL_DISCARDED => Some((
                        LogRecord::JournalDiscarded(transaction_id, journal_id),
                        value,
                    )),
                    _ => unimplemented!(),
                }
            }
            TRANSACTION_PREPARED => {
                // Prepared.
                let (instant, value) = read_part::<S::Instant>(value)?;
                Some((
                    LogRecord::TransactionPrepared(transaction_id, instant),
                    value,
                ))
            }
            TRANSACTION_COMMITTED => {
                // Committed.
                let (instant, value) = read_part::<S::Instant>(value)?;
                Some((
                    LogRecord::TransactionCommitted(transaction_id, instant),
                    value,
                ))
            }
            TRANSACTION_ROLLED_BACK => {
                // Rolled back.
                let (to, value) = read_part::<u32>(value)?;
                Some((LogRecord::TransactionRolledBack(transaction_id, to), value))
            }
            _ => unreachable!(),
        }
    }

    /// Writes the data into the supplied buffer.
    ///
    /// Returns `None` if the data could not be written to the buffer, otherwise returns the number
    /// of bytes written to the buffer.
    pub(super) fn write(&self, buffer: &mut [u8]) -> Option<usize> {
        let buffer_len = buffer.len();
        let buffer = match self {
            LogRecord::EndOfLog => write_part::<u64>(0, buffer)?,
            LogRecord::BufferSubmitted(transaction_instant) => {
                let transaction_instant_with_extended_opcode =
                    (u64::from(*transaction_instant) << 32) | BUFFER_COMMITTED;
                write_part::<u64>(transaction_instant_with_extended_opcode, buffer)?
            }
            LogRecord::BufferDiscarded => write_part::<u64>(BUFFER_ROLLED_BACK, buffer)?,
            LogRecord::JournalCreatedObjectSingle(transaction_id, journal_id, object_id) => {
                debug_assert_eq!(transaction_id & OPCODE_MASK, 0);
                debug_assert_eq!(journal_id & OPCODE_MASK, 0);
                let transaction_id_with_mark = transaction_id | TRANSACTION_UPDATED;
                let buffer = write_part::<TransactionID>(transaction_id_with_mark, buffer)?;
                let journal_id_with_opcode = journal_id | JOURNAL_CREATED_SINGLE;
                let buffer = write_part::<JournalID>(journal_id_with_opcode, buffer)?;
                write_part::<u64>(*object_id, buffer)?
            }
            LogRecord::JournalCreatedObjectRange(
                transaction_id,
                journal_id,
                object_id,
                interval,
                num_objects,
            ) => {
                debug_assert_eq!(transaction_id & OPCODE_MASK, 0);
                debug_assert_eq!(journal_id & OPCODE_MASK, 0);
                let transaction_id_with_mark = transaction_id | TRANSACTION_UPDATED;
                let buffer = write_part::<TransactionID>(transaction_id_with_mark, buffer)?;
                let journal_id_with_opcode = journal_id | JOURNAL_CREATED_RANGE;
                let buffer = write_part::<JournalID>(journal_id_with_opcode, buffer)?;
                let buffer = write_part::<u64>(*object_id, buffer)?;
                let buffer = write_part::<u32>(*interval, buffer)?;
                write_part::<u32>(*num_objects, buffer)?
            }
            LogRecord::JournalDeletedObjectSingle(transaction_id, journal_id, object_id) => {
                debug_assert_eq!(transaction_id & OPCODE_MASK, 0);
                debug_assert_eq!(journal_id & OPCODE_MASK, 0);
                let transaction_id_with_mark = transaction_id | TRANSACTION_UPDATED;
                let buffer = write_part::<TransactionID>(transaction_id_with_mark, buffer)?;
                let journal_id_with_opcode = journal_id | JOURNAL_DELETED_SINGLE;
                let buffer = write_part::<JournalID>(journal_id_with_opcode, buffer)?;
                write_part::<u64>(*object_id, buffer)?
            }
            LogRecord::JournalDeletedObjectRange(
                transaction_id,
                journal_id,
                object_id,
                interval,
                num_objects,
            ) => {
                debug_assert_eq!(transaction_id & OPCODE_MASK, 0);
                debug_assert_eq!(journal_id & OPCODE_MASK, 0);
                let transaction_id_with_mark = transaction_id | TRANSACTION_UPDATED;
                let buffer = write_part::<TransactionID>(transaction_id_with_mark, buffer)?;
                let journal_id_with_opcode = journal_id | JOURNAL_DELETED_RANGE;
                let buffer = write_part::<JournalID>(journal_id_with_opcode, buffer)?;
                let buffer = write_part::<u64>(*object_id, buffer)?;
                let buffer = write_part::<u32>(*interval, buffer)?;
                write_part::<u32>(*num_objects, buffer)?
            }
            LogRecord::JournalSubmitted(transaction_id, journal_id, transaction_instant) => {
                debug_assert_eq!(transaction_id & OPCODE_MASK, 0);
                debug_assert_eq!(journal_id & OPCODE_MASK, 0);
                let transaction_id_with_mark = transaction_id | TRANSACTION_UPDATED;
                let buffer = write_part::<TransactionID>(transaction_id_with_mark, buffer)?;
                let journal_id_with_opcode = journal_id | JOURNAL_SUBMITTED;
                let buffer = write_part::<JournalID>(journal_id_with_opcode, buffer)?;
                write_part::<u32>(*transaction_instant, buffer)?
            }
            LogRecord::JournalDiscarded(transaction_id, journal_id) => {
                debug_assert_eq!(transaction_id & OPCODE_MASK, 0);
                debug_assert_eq!(journal_id & OPCODE_MASK, 0);
                let transaction_id_with_mark = transaction_id | TRANSACTION_UPDATED;
                let buffer = write_part::<TransactionID>(transaction_id_with_mark, buffer)?;
                let journal_id_with_opcode = journal_id | JOURNAL_DISCARDED;
                write_part::<JournalID>(journal_id_with_opcode, buffer)?
            }
            LogRecord::TransactionPrepared(transaction_id, instant) => {
                debug_assert_eq!(transaction_id & OPCODE_MASK, 0);
                let transaction_id_with_mark = transaction_id | TRANSACTION_PREPARED;
                let buffer = write_part::<TransactionID>(transaction_id_with_mark, buffer)?;
                write_part::<S::Instant>(*instant, buffer)?
            }
            LogRecord::TransactionCommitted(transaction_id, instant) => {
                debug_assert_eq!(transaction_id & OPCODE_MASK, 0);
                let transaction_id_with_mark = transaction_id | TRANSACTION_COMMITTED;
                let buffer = write_part::<TransactionID>(transaction_id_with_mark, buffer)?;
                write_part::<S::Instant>(*instant, buffer)?
            }
            LogRecord::TransactionRolledBack(transaction_id, to) => {
                debug_assert_eq!(transaction_id & OPCODE_MASK, 0);
                let transaction_id_with_mark = transaction_id | TRANSACTION_ROLLED_BACK;
                let buffer = write_part::<TransactionID>(transaction_id_with_mark, buffer)?;
                write_part::<u32>(*to, buffer)?
            }
        };
        Some(buffer_len - buffer.len())
    }
}

impl<S: Sequencer> PartialEq for LogRecord<S> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::EndOfLog, Self::EndOfLog) | (Self::BufferDiscarded, Self::BufferDiscarded) => {
                true
            }
            (Self::BufferSubmitted(l0), Self::BufferSubmitted(r0)) => l0 == r0,
            (
                Self::JournalCreatedObjectSingle(l0, l1, l2),
                Self::JournalCreatedObjectSingle(r0, r1, r2),
            )
            | (
                Self::JournalDeletedObjectSingle(l0, l1, l2),
                Self::JournalDeletedObjectSingle(r0, r1, r2),
            ) => l0 == r0 && l1 == r1 && l2 == r2,
            (
                Self::JournalCreatedObjectRange(l0, l1, l2, l3, l4),
                Self::JournalCreatedObjectRange(r0, r1, r2, r3, r4),
            )
            | (
                Self::JournalDeletedObjectRange(l0, l1, l2, l3, l4),
                Self::JournalDeletedObjectRange(r0, r1, r2, r3, r4),
            ) => l0 == r0 && l1 == r1 && l2 == r2 && l3 == r3 && l4 == r4,
            (Self::JournalSubmitted(l0, l1, l2), Self::JournalSubmitted(r0, r1, r2)) => {
                l0 == r0 && l1 == r1 && l2 == r2
            }
            (Self::JournalDiscarded(l0, l1), Self::JournalDiscarded(r0, r1)) => {
                l0 == r0 && l1 == r1
            }
            (Self::TransactionPrepared(l0, l1), Self::TransactionPrepared(r0, r1))
            | (Self::TransactionCommitted(l0, l1), Self::TransactionCommitted(r0, r1)) => {
                l0 == r0 && l1 == r1
            }
            (Self::TransactionRolledBack(l0, l1), Self::TransactionRolledBack(r0, r1)) => {
                l0 == r0 && l1 == r1
            }
            _ => false,
        }
    }
}

fn read_part<T: Copy + Sized>(value: &[u8]) -> Option<(T, &[u8])> {
    if value.len() < size_of::<T>() {
        return None;
    }
    let mut uninit_t = MaybeUninit::<T>::uninit();
    let bytes: *mut u8 = uninit_t.as_mut_ptr().cast::<u8>();
    (0..size_of::<T>()).for_each(|i| {
        // Safety: the length of the data is checked.
        unsafe {
            *bytes.add(i) = value[i];
        }
    });
    // Safety: `T` is `Copy`.
    unsafe { Some((uninit_t.assume_init(), &value[size_of::<T>()..])) }
}

fn write_part<T: Copy + Sized>(value: T, buffer: &mut [u8]) -> Option<&mut [u8]> {
    if buffer.len() < size_of::<T>() {
        return None;
    }
    let bytes: *const u8 = addr_of!(value).cast::<u8>();
    (0..size_of::<T>()).for_each(|i| {
        // Safety: the length of the data is checked.
        unsafe {
            buffer[i] = *bytes.add(i);
        }
    });
    Some(&mut buffer[size_of::<T>()..])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{utils::IntHasher, MonotonicU64};
    use proptest::prelude::*;
    use std::hash::{Hash, Hasher};

    proptest! {
        #[test]
        fn read_write(seed in 0_u64..u64::MAX) {
            let mut hasher = IntHasher::default();
            let transaction_opcode = seed & OPCODE_MASK;
            let transaction_id = seed & (!OPCODE_MASK);
            (seed.rotate_left(32)).hash(&mut hasher);
            let instant = hasher.finish();
            let mut small_buffer = [0; 5];
            let mut medium_buffer = [0; 12];
            let mut large_buffer = [0; 32];

            match transaction_opcode {
                0 => {
                    let extended_opcode = seed & EXTENDED_OPCODE_MASK;
                    match extended_opcode {
                        0 => {
                            let eol = LogRecord::<MonotonicU64>::EndOfLog;
                            assert!(eol.write(&mut small_buffer).is_none());
                            assert!(eol.write(&mut medium_buffer).is_some());
                            assert!(eol.write(&mut large_buffer).is_some());
                            if let Some((recovered_from_medium, _)) = LogRecord::<MonotonicU64>::from_raw_data(&medium_buffer) {
                                if let Some((recovered_from_large, _)) = LogRecord::<MonotonicU64>::from_raw_data(&large_buffer) {
                                    assert_eq!(recovered_from_large, eol);
                                    assert_eq!(recovered_from_medium, eol);
                                } else {
                                    unreachable!();
                                }
                            } else {
                                unreachable!();
                            }
                        }
                        BUFFER_COMMITTED => {
                            let transaction_instant: u32 = (seed >> 32).try_into().ok().map_or(0, |v| v);
                            let buffer_committed = LogRecord::<MonotonicU64>::BufferSubmitted(transaction_instant);
                            assert!(buffer_committed.write(&mut small_buffer).is_none());
                            assert!(buffer_committed.write(&mut medium_buffer).is_some());
                            assert!(buffer_committed.write(&mut large_buffer).is_some());
                            if let Some((recovered_from_medium, _)) = LogRecord::<MonotonicU64>::from_raw_data(&medium_buffer) {
                                if let Some((recovered_from_large, _)) = LogRecord::<MonotonicU64>::from_raw_data(&large_buffer) {
                                    assert_eq!(recovered_from_large, buffer_committed);
                                    assert_eq!(recovered_from_medium, buffer_committed);
                                } else {
                                    unreachable!();
                                }
                            } else {
                                unreachable!();
                            }
                        }
                        BUFFER_ROLLED_BACK => {
                            let buffer_rolled_back = LogRecord::<MonotonicU64>::BufferDiscarded;
                            assert!(buffer_rolled_back.write(&mut small_buffer).is_none());
                            assert!(buffer_rolled_back.write(&mut medium_buffer).is_some());
                            assert!(buffer_rolled_back.write(&mut large_buffer).is_some());
                            if let Some((recovered_from_medium, _)) = LogRecord::<MonotonicU64>::from_raw_data(&medium_buffer) {
                                if let Some((recovered_from_large, _)) = LogRecord::<MonotonicU64>::from_raw_data(&large_buffer) {
                                    assert_eq!(recovered_from_large, buffer_rolled_back);
                                    assert_eq!(recovered_from_medium, buffer_rolled_back);
                                } else {
                                    unreachable!();
                                }
                            } else {
                                unreachable!();
                            }
                        }
                        _ => (),
                    }
                }
                TRANSACTION_UPDATED => {
                    seed.hash(&mut hasher);
                    let hash = hasher.finish();
                    let journal_id = hash & (!OPCODE_MASK);
                    let opcode = hash & OPCODE_MASK;
                    match opcode {
                        JOURNAL_CREATED_SINGLE => {
                            let created = LogRecord::<MonotonicU64>::JournalCreatedObjectSingle(transaction_id, journal_id, hash);
                            assert!(created.write(&mut small_buffer).is_none());
                            assert!(created.write(&mut medium_buffer).is_none());
                            assert!(created.write(&mut large_buffer).is_some());
                            if let Some((recovered, _)) = LogRecord::<MonotonicU64>::from_raw_data(&large_buffer) {
                                assert_eq!(recovered, created);
                            } else {
                                unreachable!();
                            }
                        }
                        JOURNAL_CREATED_RANGE => {
                            let created = LogRecord::<MonotonicU64>::JournalCreatedObjectRange(transaction_id, journal_id, hash, (hash >> 32) as u32, hash as u32);
                            assert!(created.write(&mut small_buffer).is_none());
                            assert!(created.write(&mut medium_buffer).is_none());
                            assert!(created.write(&mut large_buffer).is_some());
                            if let Some((recovered, _)) = LogRecord::<MonotonicU64>::from_raw_data(&large_buffer) {
                                assert_eq!(recovered, created);
                            } else {
                                unreachable!();
                            }
                        }
                        JOURNAL_DELETED_SINGLE => {
                            let deleted = LogRecord::<MonotonicU64>::JournalDeletedObjectSingle(transaction_id, journal_id, hash);
                            assert!(deleted.write(&mut small_buffer).is_none());
                            assert!(deleted.write(&mut medium_buffer).is_none());
                            assert!(deleted.write(&mut large_buffer).is_some());
                            if let Some((recovered, _)) = LogRecord::<MonotonicU64>::from_raw_data(&large_buffer) {
                                assert_eq!(recovered, deleted);
                            } else {
                                unreachable!();
                            }
                        }
                        JOURNAL_DELETED_RANGE => {
                            let deleted = LogRecord::<MonotonicU64>::JournalDeletedObjectRange(transaction_id, journal_id, hash, (hash >> 32) as u32, hash as u32);
                            assert!(deleted.write(&mut small_buffer).is_none());
                            assert!(deleted.write(&mut medium_buffer).is_none());
                            assert!(deleted.write(&mut large_buffer).is_some());
                            if let Some((recovered, _)) = LogRecord::<MonotonicU64>::from_raw_data(&large_buffer) {
                                assert_eq!(recovered, deleted);
                            } else {
                                unreachable!();
                            }
                        }
                        JOURNAL_SUBMITTED => {
                            let submitted = LogRecord::<MonotonicU64>::JournalSubmitted(transaction_id, journal_id, hash.try_into().ok().map_or(0, |v| v));
                            assert!(submitted.write(&mut small_buffer).is_none());
                            assert!(submitted.write(&mut medium_buffer).is_none());
                            assert!(submitted.write(&mut large_buffer).is_some());
                            if let Some((recovered, _)) = LogRecord::<MonotonicU64>::from_raw_data(&large_buffer) {
                                assert_eq!(recovered, submitted);
                            } else {
                                unreachable!();
                            }
                        }
                        JOURNAL_DISCARDED => {
                            let discarded = LogRecord::<MonotonicU64>::JournalDiscarded(transaction_id, journal_id);
                            assert!(discarded.write(&mut small_buffer).is_none());
                            assert!(discarded.write(&mut medium_buffer).is_none());
                            assert!(discarded.write(&mut large_buffer).is_some());
                            if let Some((recovered, _)) = LogRecord::<MonotonicU64>::from_raw_data(&large_buffer) {
                                assert_eq!(recovered, discarded);
                            } else {
                                unreachable!();
                            }
                        }
                        _ => (),
                    }
                }
                TRANSACTION_PREPARED => {
                    let prepared = LogRecord::<MonotonicU64>::TransactionPrepared(transaction_id, instant);
                    assert!(prepared.write(&mut small_buffer).is_none());
                    assert!(prepared.write(&mut medium_buffer).is_none());
                    assert!(prepared.write(&mut large_buffer).is_some());
                    if let Some((recovered, _)) = LogRecord::<MonotonicU64>::from_raw_data(&large_buffer) {
                        assert_eq!(recovered, prepared);
                    } else {
                        unreachable!();
                    }
                }
                TRANSACTION_COMMITTED => {
                    let committed = LogRecord::<MonotonicU64>::TransactionCommitted(transaction_id, instant);
                    assert!(committed.write(&mut small_buffer).is_none());
                    assert!(committed.write(&mut medium_buffer).is_none());
                    assert!(committed.write(&mut large_buffer).is_some());
                    if let Some((recovered, _)) = LogRecord::<MonotonicU64>::from_raw_data(&large_buffer) {
                        assert_eq!(recovered, committed);
                    } else {
                        unreachable!();
                    }
                }
                TRANSACTION_ROLLED_BACK => {
                    let rolled_back = LogRecord::<MonotonicU64>::TransactionRolledBack(transaction_id, 1);
                    assert!(rolled_back.write(&mut small_buffer).is_none());
                    assert!(rolled_back.write(&mut medium_buffer).is_some());
                    assert!(rolled_back.write(&mut large_buffer).is_some());
                    if let Some((recovered_from_medium, _)) = LogRecord::<MonotonicU64>::from_raw_data(&medium_buffer) {
                        if let Some((recovered_from_large, _)) = LogRecord::<MonotonicU64>::from_raw_data(&large_buffer) {
                            assert_eq!(recovered_from_large, rolled_back);
                            assert_eq!(recovered_from_medium, rolled_back);
                        } else {
                            unreachable!();
                        }
                    } else {
                        unreachable!();
                    }
                }
                _ => (),
            }
        }
    }
}
