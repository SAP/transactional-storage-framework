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
/// The maximum size of a log record is 32-byte.
///
/// The byte representation of [`LogRecord`] is as follows.
/// - 62-bit transaction ID, 2-bit EOT marker.
/// - If EOT = 00, TODO: define it.
/// - If EOT = 01, the transaction is being prepared for commit, and `S::Instant` follows.
/// - If EOT = 10, the transaction is being committed, and `S::Instant` follows.
/// - If EOT = 11, the transaction is being rolled back.
#[derive(Copy, Clone, Debug, Eq)]
pub(super) enum LogRecord<S: Sequencer> {
    /// End-of-log marker.
    ///
    /// Consecutive eight `0`s represent the end of a log file.
    EndOfLog,

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

impl<S: Sequencer> LogRecord<S> {
    /// Tries to parse the supplied `u8` slice as a [`LogRecord`].
    pub(super) fn from_raw_data(value: &[u8]) -> Option<(Self, &[u8])> {
        let (transaction_id_with_mark, value) = read_part::<TransactionID>(value)?;

        if transaction_id_with_mark == 0 {
            return Some((Self::EndOfLog, value));
        }

        let eot_mark = transaction_id_with_mark & 0b11;
        let transaction_id = transaction_id_with_mark & (!0b11);
        match eot_mark {
            0b00 => {
                let (journal_id_with_opcode, value) = read_part::<JournalID>(value)?;
                let journal_id = journal_id_with_opcode & (!0b11);
                let opcode = journal_id_with_opcode & 0b11;
                match opcode {
                    0b00 => {
                        // Created.
                        let (data_id, value) = read_part::<u64>(value)?;
                        Some((
                            LogRecord::Created(transaction_id, journal_id, data_id),
                            value,
                        ))
                    }
                    0b01 => {
                        // Created two.
                        let (data_id_1, value) = read_part::<u64>(value)?;
                        let (data_id_2, value) = read_part::<u64>(value)?;
                        Some((
                            LogRecord::CreatedTwo(transaction_id, journal_id, data_id_1, data_id_2),
                            value,
                        ))
                    }
                    0b10 => {
                        // Deleted.
                        let (data_id, value) = read_part::<u64>(value)?;
                        Some((
                            LogRecord::Deleted(transaction_id, journal_id, data_id),
                            value,
                        ))
                    }
                    0b11 => {
                        // Deleted two.
                        let (data_id_1, value) = read_part::<u64>(value)?;
                        let (data_id_2, value) = read_part::<u64>(value)?;
                        Some((
                            LogRecord::DeletedTwo(transaction_id, journal_id, data_id_1, data_id_2),
                            value,
                        ))
                    }
                    _ => unreachable!(),
                }
            }
            0b01 => {
                // Prepared.
                let (instant, value) = read_part::<S::Instant>(value)?;
                Some((LogRecord::Prepared(transaction_id, instant), value))
            }
            0b10 => {
                // Committed.
                let (instant, value) = read_part::<S::Instant>(value)?;
                Some((LogRecord::Committed(transaction_id, instant), value))
            }
            0b11 => {
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
            LogRecord::Created(transaction_id, journal_id, data_id) => {
                debug_assert_eq!(transaction_id & 0b11, 0);
                debug_assert_eq!(journal_id & 0b11, 0);
                let buffer = write_part::<TransactionID>(*transaction_id, buffer)?;
                let buffer = write_part::<JournalID>(*journal_id, buffer)?;
                write_part::<u64>(*data_id, buffer)?
            }
            LogRecord::CreatedTwo(transaction_id, journal_id, data_id_1, data_id_2) => {
                debug_assert_eq!(transaction_id & 0b11, 0);
                debug_assert_eq!(journal_id & 0b11, 0);
                let buffer = write_part::<TransactionID>(*transaction_id, buffer)?;
                let journal_id_with_opcode = journal_id | 0b01;
                let buffer = write_part::<JournalID>(journal_id_with_opcode, buffer)?;
                let buffer = write_part::<u64>(*data_id_1, buffer)?;
                write_part::<u64>(*data_id_2, buffer)?
            }
            LogRecord::Deleted(transaction_id, journal_id, data_id) => {
                debug_assert_eq!(transaction_id & 0b11, 0);
                debug_assert_eq!(journal_id & 0b11, 0);
                let buffer = write_part::<TransactionID>(*transaction_id, buffer)?;
                let journal_id_with_opcode = journal_id | 0b10;
                let buffer = write_part::<JournalID>(journal_id_with_opcode, buffer)?;
                write_part::<u64>(*data_id, buffer)?
            }
            LogRecord::DeletedTwo(transaction_id, journal_id, data_id_1, data_id_2) => {
                debug_assert_eq!(transaction_id & 0b11, 0);
                debug_assert_eq!(journal_id & 0b11, 0);
                let buffer = write_part::<TransactionID>(*transaction_id, buffer)?;
                let journal_id_with_opcode = journal_id | 0b11;
                let buffer = write_part::<JournalID>(journal_id_with_opcode, buffer)?;
                let buffer = write_part::<u64>(*data_id_1, buffer)?;
                write_part::<u64>(*data_id_2, buffer)?
            }
            LogRecord::Prepared(transaction_id, instant) => {
                debug_assert_eq!(transaction_id & 0b11, 0);
                let eot_mark = 0b01;
                let transaction_id_with_mark = transaction_id | eot_mark;
                let buffer = write_part::<TransactionID>(transaction_id_with_mark, buffer)?;
                write_part::<S::Instant>(*instant, buffer)?
            }
            LogRecord::Committed(transaction_id, instant) => {
                debug_assert_eq!(transaction_id & 0b11, 0);
                let eot_mark = 0b10;
                let transaction_id_with_mark = transaction_id | eot_mark;
                let buffer = write_part::<TransactionID>(transaction_id_with_mark, buffer)?;
                write_part::<S::Instant>(*instant, buffer)?
            }
            LogRecord::RolledBack(transaction_id, to) => {
                debug_assert_eq!(transaction_id & 0b11, 0);
                let eot_mark = 0b11;
                let transaction_id_with_mark = transaction_id | eot_mark;
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
            (Self::EndOfLog, Self::EndOfLog) => true,
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
    use crate::{utils::IntHasher, AtomicCounter};
    use proptest::prelude::*;
    use std::hash::{Hash, Hasher};

    proptest! {
        #[test]
        fn read_write(seed in 0_usize..usize::MAX) {
            let mut hasher = IntHasher::default();
            let eot_mark = seed % 0b11;
            let transaction_id = (seed & (!0b11)) as u64;
            (seed.rotate_left(32)).hash(&mut hasher);
            let instant = hasher.finish();
            let mut small_buffer = [0; 5];
            let mut medium_buffer = [0; 12];
            let mut large_buffer = [0; 32];

            let result = match eot_mark {
                0b00 => {
                    seed.hash(&mut hasher);
                    let hash = hasher.finish();
                    let journal_id = hash & (!0b11);
                    let opcode = hash & 0b11;
                    match opcode {
                        0b00 => {
                            let created = LogRecord::<AtomicCounter>::Created(transaction_id, journal_id, hash);
                            assert!(created.write(&mut small_buffer).is_none());
                            assert!(created.write(&mut medium_buffer).is_none());
                            assert!(created.write(&mut large_buffer).is_some());
                            if let Some((recovered, _)) = LogRecord::<AtomicCounter>::from_raw_data(&large_buffer) {
                                recovered == created
                            } else {
                                false
                            }
                        }
                        0b01 => {
                            let created = LogRecord::<AtomicCounter>::CreatedTwo(transaction_id, journal_id, hash, hash.rotate_left(32));
                            assert!(created.write(&mut small_buffer).is_none());
                            assert!(created.write(&mut medium_buffer).is_none());
                            assert!(created.write(&mut large_buffer).is_some());
                            if let Some((recovered, _)) = LogRecord::<AtomicCounter>::from_raw_data(&large_buffer) {
                                recovered == created
                            } else {
                                false
                            }
                        }
                        0b10 => {
                            let deleted = LogRecord::<AtomicCounter>::Deleted(transaction_id, journal_id, hash);
                            assert!(deleted.write(&mut small_buffer).is_none());
                            assert!(deleted.write(&mut medium_buffer).is_none());
                            assert!(deleted.write(&mut large_buffer).is_some());
                            if let Some((recovered, _)) = LogRecord::<AtomicCounter>::from_raw_data(&large_buffer) {
                                recovered == deleted
                            } else {
                                false
                            }
                        }
                        0b11 => {
                            let deleted = LogRecord::<AtomicCounter>::DeletedTwo(transaction_id, journal_id, hash, hash.rotate_left(32));
                            assert!(deleted.write(&mut small_buffer).is_none());
                            assert!(deleted.write(&mut medium_buffer).is_none());
                            assert!(deleted.write(&mut large_buffer).is_some());
                            if let Some((recovered, _)) = LogRecord::<AtomicCounter>::from_raw_data(&large_buffer) {
                                recovered == deleted
                            } else {
                                false
                            }
                        }
                        _ => unreachable!(),
                    }
                }
                0b01 => {
                    let prepared = LogRecord::<AtomicCounter>::Prepared(transaction_id, instant);
                    assert!(prepared.write(&mut small_buffer).is_none());
                    assert!(prepared.write(&mut medium_buffer).is_none());
                    assert!(prepared.write(&mut large_buffer).is_some());
                    if let Some((recovered, _)) = LogRecord::<AtomicCounter>::from_raw_data(&large_buffer) {
                        recovered == prepared
                    } else {
                        false
                    }
                }
                0b10 => {
                    let committed = LogRecord::<AtomicCounter>::Committed(transaction_id, instant);
                    assert!(committed.write(&mut small_buffer).is_none());
                    assert!(committed.write(&mut medium_buffer).is_none());
                    assert!(committed.write(&mut large_buffer).is_some());
                    if let Some((recovered, _)) = LogRecord::<AtomicCounter>::from_raw_data(&large_buffer) {
                        recovered == committed
                    } else {
                        false
                    }
                }
                0b11 => {
                    let rolled_back = LogRecord::<AtomicCounter>::RolledBack(transaction_id, 1);
                    assert!(rolled_back.write(&mut small_buffer).is_none());
                    assert!(rolled_back.write(&mut medium_buffer).is_some());
                    assert!(rolled_back.write(&mut large_buffer).is_some());
                    if let Some((recovered_from_medium, _)) = LogRecord::<AtomicCounter>::from_raw_data(&medium_buffer) {
                        if let Some((recovered_from_large, _)) = LogRecord::<AtomicCounter>::from_raw_data(&large_buffer) {
                            recovered_from_large == rolled_back && recovered_from_medium == rolled_back
                        } else {
                            false
                        }
                    } else {
                        false
                    }
                }
                _ => unreachable!(),
            };
            assert!(result);
        }
    }
}
