// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use crate::journal::ID as JournalID;
use crate::transaction::ID as TransactionID;
use crate::Sequencer;
use std::mem::{size_of, MaybeUninit};
use std::ptr::addr_of;

/// Individual log record.
///
/// The maximum size of a log record is limited to 32-byte when the size of `S::Instant` is not
/// larger than 24-byte.
///
/// The byte representation of [`LogRecord`] is as follows.
/// - 61-bit transaction ID, and 3-bit transaction control opcode follows.
/// - If `transaction opcode = 0b000`, the event is unrelated to a transaction.
/// -- `0x0000000000000000`: the end of log file.
/// -- `0x0000000FF0000000`: the buffer was committed.
/// -- `0x000000FFFF000000`: the buffer was rolled back.
/// -- TODO: page reorganization.
/// - If `transaction opcode = 0b100`, the event happened in a transaction.
/// -- 61-bit journal ID, 3-bit opcode.
/// -- If `opcode = 0b000`, the journal created data identified as the `u64` value that follows.
/// -- If `opcode = 0b001`, the journal created data identified as the two `u64` values that follow.
/// -- If `opcode = 0b010`, the journal deleted data identified as the `u64` value that follows.
/// -- If `opcode = 0b011`, the journal deleted data identified as the two `u64` values that follow.
/// - If `transaction opcode = 0b101`, the transaction is being prepared for commit, and `S::Instant` follows.
/// - If `transaction opcode = 0b110`, the transaction is being committed, and `S::Instant` follows.
/// - If `transaction opcode = 0b111`, the transaction is being rolled back.
#[derive(Copy, Clone, Debug, Eq)]
pub(super) enum LogRecord<S: Sequencer> {
    /// End-of-log marker.
    ///
    /// Consecutive eight `0`s represent the end of a log file.
    EndOfLog,

    /// The log buffer was committed.
    BufferCommitted,

    /// The log buffer was rolled back.
    BufferRolledBack,

    /// The transaction created a database object identified as the `u64` value.
    Created(TransactionID, JournalID, u64),

    /// The transaction created two database objects identified as the two `u64` values.
    CreatedTwo(TransactionID, JournalID, u64, u64),

    /// The transaction deleted a database object identified as the `u64` value.
    Deleted(TransactionID, JournalID, u64),

    /// The transaction deleted two database objects identified as the two `u64` values.
    DeletedTwo(TransactionID, JournalID, u64, u64),

    /// The transaction is prepared.
    Prepared(TransactionID, S::Instant),

    /// The transaction is committed.
    Committed(TransactionID, S::Instant),

    /// The transaction is rolled back.
    RolledBack(TransactionID, u32),
}

/// The log buffer was committed.
pub const BUFFER_COMMITTED: u64 = 0x0000_00FF_FF00_0000;

/// The log buffer was rolled back.
pub const BUFFER_ROLLED_BACK: u64 = 0x0000_FFFF_FFFF_0000;

/// Opcode bit mask.
pub const OPCODE_MASK: u64 = 0b111;

/// The journal created a database object.
pub const JOURNAL_CREATED_ONE: u64 = 0b000;

/// The journal created two database objects.
pub const JOURNAL_CREATED_TWO: u64 = 0b001;

/// The journal deleted a database object.
pub const JOURNAL_DELETED_ONE: u64 = 0b010;

/// The journal deleted two database objects.
pub const JOURNAL_DELETED_TWO: u64 = 0b011;

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
    pub(super) fn from_raw_data(value: &[u8]) -> Option<(Self, &[u8])> {
        let (transaction_id_with_opcode, value) = read_part::<TransactionID>(value)?;

        let transaction_opcode = transaction_id_with_opcode & OPCODE_MASK;
        if transaction_opcode == 0 {
            match transaction_id_with_opcode {
                0 => {
                    return Some((Self::EndOfLog, value));
                }
                BUFFER_COMMITTED => {
                    return Some((LogRecord::BufferCommitted, value));
                }
                BUFFER_ROLLED_BACK => {
                    return Some((LogRecord::BufferRolledBack, value));
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
                    JOURNAL_CREATED_ONE => {
                        // Created.
                        let (object_id, value) = read_part::<u64>(value)?;
                        Some((
                            LogRecord::Created(transaction_id, journal_id, object_id),
                            value,
                        ))
                    }
                    JOURNAL_CREATED_TWO => {
                        // Created two.
                        let (object_id_1, value) = read_part::<u64>(value)?;
                        let (object_id_2, value) = read_part::<u64>(value)?;
                        Some((
                            LogRecord::CreatedTwo(
                                transaction_id,
                                journal_id,
                                object_id_1,
                                object_id_2,
                            ),
                            value,
                        ))
                    }
                    JOURNAL_DELETED_ONE => {
                        // Deleted.
                        let (object_id, value) = read_part::<u64>(value)?;
                        Some((
                            LogRecord::Deleted(transaction_id, journal_id, object_id),
                            value,
                        ))
                    }
                    JOURNAL_DELETED_TWO => {
                        // Deleted two.
                        let (object_id_1, value) = read_part::<u64>(value)?;
                        let (object_id_2, value) = read_part::<u64>(value)?;
                        Some((
                            LogRecord::DeletedTwo(
                                transaction_id,
                                journal_id,
                                object_id_1,
                                object_id_2,
                            ),
                            value,
                        ))
                    }
                    _ => unimplemented!(),
                }
            }
            TRANSACTION_PREPARED => {
                // Prepared.
                let (instant, value) = read_part::<S::Instant>(value)?;
                Some((LogRecord::Prepared(transaction_id, instant), value))
            }
            TRANSACTION_COMMITTED => {
                // Committed.
                let (instant, value) = read_part::<S::Instant>(value)?;
                Some((LogRecord::Committed(transaction_id, instant), value))
            }
            TRANSACTION_ROLLED_BACK => {
                // Rolled back.
                let (to, value) = read_part::<u32>(value)?;
                Some((LogRecord::RolledBack(transaction_id, to), value))
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
            LogRecord::EndOfLog => return None,
            LogRecord::BufferCommitted => write_part::<u64>(BUFFER_COMMITTED, buffer)?,
            LogRecord::BufferRolledBack => write_part::<u64>(BUFFER_ROLLED_BACK, buffer)?,
            LogRecord::Created(transaction_id, journal_id, object_id) => {
                debug_assert_eq!(transaction_id & OPCODE_MASK, 0);
                debug_assert_eq!(journal_id & OPCODE_MASK, 0);
                let transaction_id_with_mark = transaction_id | TRANSACTION_UPDATED;
                let buffer = write_part::<TransactionID>(transaction_id_with_mark, buffer)?;
                let journal_id_with_opcode = journal_id | JOURNAL_CREATED_ONE;
                let buffer = write_part::<JournalID>(journal_id_with_opcode, buffer)?;
                write_part::<u64>(*object_id, buffer)?
            }
            LogRecord::CreatedTwo(transaction_id, journal_id, object_id_1, object_id_2) => {
                debug_assert_eq!(transaction_id & OPCODE_MASK, 0);
                debug_assert_eq!(journal_id & OPCODE_MASK, 0);
                let transaction_id_with_mark = transaction_id | TRANSACTION_UPDATED;
                let buffer = write_part::<TransactionID>(transaction_id_with_mark, buffer)?;
                let journal_id_with_opcode = journal_id | JOURNAL_CREATED_TWO;
                let buffer = write_part::<JournalID>(journal_id_with_opcode, buffer)?;
                let buffer = write_part::<u64>(*object_id_1, buffer)?;
                write_part::<u64>(*object_id_2, buffer)?
            }
            LogRecord::Deleted(transaction_id, journal_id, object_id) => {
                debug_assert_eq!(transaction_id & OPCODE_MASK, 0);
                debug_assert_eq!(journal_id & OPCODE_MASK, 0);
                let transaction_id_with_mark = transaction_id | TRANSACTION_UPDATED;
                let buffer = write_part::<TransactionID>(transaction_id_with_mark, buffer)?;
                let journal_id_with_opcode = journal_id | JOURNAL_DELETED_ONE;
                let buffer = write_part::<JournalID>(journal_id_with_opcode, buffer)?;
                write_part::<u64>(*object_id, buffer)?
            }
            LogRecord::DeletedTwo(transaction_id, journal_id, object_id_1, object_id_2) => {
                debug_assert_eq!(transaction_id & OPCODE_MASK, 0);
                debug_assert_eq!(journal_id & OPCODE_MASK, 0);
                let transaction_id_with_mark = transaction_id | TRANSACTION_UPDATED;
                let buffer = write_part::<TransactionID>(transaction_id_with_mark, buffer)?;
                let journal_id_with_opcode = journal_id | JOURNAL_DELETED_TWO;
                let buffer = write_part::<JournalID>(journal_id_with_opcode, buffer)?;
                let buffer = write_part::<u64>(*object_id_1, buffer)?;
                write_part::<u64>(*object_id_2, buffer)?
            }
            LogRecord::Prepared(transaction_id, instant) => {
                debug_assert_eq!(transaction_id & OPCODE_MASK, 0);
                let transaction_id_with_mark = transaction_id | TRANSACTION_PREPARED;
                let buffer = write_part::<TransactionID>(transaction_id_with_mark, buffer)?;
                write_part::<S::Instant>(*instant, buffer)?
            }
            LogRecord::Committed(transaction_id, instant) => {
                debug_assert_eq!(transaction_id & OPCODE_MASK, 0);
                let transaction_id_with_mark = transaction_id | TRANSACTION_COMMITTED;
                let buffer = write_part::<TransactionID>(transaction_id_with_mark, buffer)?;
                write_part::<S::Instant>(*instant, buffer)?
            }
            LogRecord::RolledBack(transaction_id, to) => {
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
            (Self::EndOfLog, Self::EndOfLog)
            | (Self::BufferCommitted, Self::BufferCommitted)
            | (Self::BufferRolledBack, Self::BufferRolledBack) => true,
            (Self::Created(l0, l1, l2), Self::Created(r0, r1, r2))
            | (Self::Deleted(l0, l1, l2), Self::Deleted(r0, r1, r2)) => {
                l0 == r0 && l1 == r1 && l2 == r2
            }
            (Self::CreatedTwo(l0, l1, l2, l3), Self::CreatedTwo(r0, r1, r2, r3))
            | (Self::DeletedTwo(l0, l1, l2, l3), Self::DeletedTwo(r0, r1, r2, r3)) => {
                l0 == r0 && l1 == r1 && ((l2 == r2 && l3 == r3) || (l2 == r3) && (l3 == r2))
            }
            (Self::Prepared(l0, l1), Self::Prepared(r0, r1))
            | (Self::Committed(l0, l1), Self::Committed(r0, r1)) => l0 == r0 && l1 == r1,
            (Self::RolledBack(l0, l1), Self::RolledBack(r0, r1)) => l0 == r0 && l1 == r1,
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

            let result = match transaction_opcode {
                TRANSACTION_UPDATED => {
                    seed.hash(&mut hasher);
                    let hash = hasher.finish();
                    let journal_id = hash & (!OPCODE_MASK);
                    let opcode = hash & OPCODE_MASK;
                    match opcode {
                        JOURNAL_CREATED_ONE => {
                            let created = LogRecord::<MonotonicU64>::Created(transaction_id, journal_id, hash);
                            assert!(created.write(&mut small_buffer).is_none());
                            assert!(created.write(&mut medium_buffer).is_none());
                            assert!(created.write(&mut large_buffer).is_some());
                            if let Some((recovered, _)) = LogRecord::<MonotonicU64>::from_raw_data(&large_buffer) {
                                recovered == created
                            } else {
                                false
                            }
                        }
                        JOURNAL_CREATED_TWO => {
                            let created = LogRecord::<MonotonicU64>::CreatedTwo(transaction_id, journal_id, hash, hash.rotate_left(32));
                            assert!(created.write(&mut small_buffer).is_none());
                            assert!(created.write(&mut medium_buffer).is_none());
                            assert!(created.write(&mut large_buffer).is_some());
                            if let Some((recovered, _)) = LogRecord::<MonotonicU64>::from_raw_data(&large_buffer) {
                                recovered == created
                            } else {
                                false
                            }
                        }
                        JOURNAL_DELETED_ONE => {
                            let deleted = LogRecord::<MonotonicU64>::Deleted(transaction_id, journal_id, hash);
                            assert!(deleted.write(&mut small_buffer).is_none());
                            assert!(deleted.write(&mut medium_buffer).is_none());
                            assert!(deleted.write(&mut large_buffer).is_some());
                            if let Some((recovered, _)) = LogRecord::<MonotonicU64>::from_raw_data(&large_buffer) {
                                recovered == deleted
                            } else {
                                false
                            }
                        }
                        JOURNAL_DELETED_TWO => {
                            let deleted = LogRecord::<MonotonicU64>::DeletedTwo(transaction_id, journal_id, hash, hash.rotate_left(32));
                            assert!(deleted.write(&mut small_buffer).is_none());
                            assert!(deleted.write(&mut medium_buffer).is_none());
                            assert!(deleted.write(&mut large_buffer).is_some());
                            if let Some((recovered, _)) = LogRecord::<MonotonicU64>::from_raw_data(&large_buffer) {
                                recovered == deleted
                            } else {
                                false
                            }
                        }
                        _ => true,
                    }
                }
                TRANSACTION_PREPARED => {
                    let prepared = LogRecord::<MonotonicU64>::Prepared(transaction_id, instant);
                    assert!(prepared.write(&mut small_buffer).is_none());
                    assert!(prepared.write(&mut medium_buffer).is_none());
                    assert!(prepared.write(&mut large_buffer).is_some());
                    if let Some((recovered, _)) = LogRecord::<MonotonicU64>::from_raw_data(&large_buffer) {
                        recovered == prepared
                    } else {
                        false
                    }
                }
                TRANSACTION_COMMITTED => {
                    let committed = LogRecord::<MonotonicU64>::Committed(transaction_id, instant);
                    assert!(committed.write(&mut small_buffer).is_none());
                    assert!(committed.write(&mut medium_buffer).is_none());
                    assert!(committed.write(&mut large_buffer).is_some());
                    if let Some((recovered, _)) = LogRecord::<MonotonicU64>::from_raw_data(&large_buffer) {
                        recovered == committed
                    } else {
                        false
                    }
                }
                TRANSACTION_ROLLED_BACK => {
                    let rolled_back = LogRecord::<MonotonicU64>::RolledBack(transaction_id, 1);
                    assert!(rolled_back.write(&mut small_buffer).is_none());
                    assert!(rolled_back.write(&mut medium_buffer).is_some());
                    assert!(rolled_back.write(&mut large_buffer).is_some());
                    if let Some((recovered_from_medium, _)) = LogRecord::<MonotonicU64>::from_raw_data(&medium_buffer) {
                        if let Some((recovered_from_large, _)) = LogRecord::<MonotonicU64>::from_raw_data(&large_buffer) {
                            recovered_from_large == rolled_back && recovered_from_medium == rolled_back
                        } else {
                            false
                        }
                    } else {
                        false
                    }
                }
                _ => true,
            };
            assert!(result);
        }
    }
}
