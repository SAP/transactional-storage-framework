// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

use crate::transaction::ID as TransactionID;
use crate::Sequencer;
use std::mem::{size_of, MaybeUninit};
use std::ptr::addr_of;

/// Individual log record.
///
/// The byte representation of [`LogRecord`] is as follows.
/// - 62-bit transaction ID, 2-bit EOT marker.
/// - If EOT = 00, TODO: define it.
/// - If EOT = 01, the transaction is being prepared for commit, and `S::Instant` follows.
/// - If EOT = 10, the transaction is being committed, and `S::Instant` follows.
/// - If EOT = 11, the transaction is being rolled back.
#[derive(Copy, Clone, Debug, Eq)]
pub(super) enum LogRecord<S: Sequencer> {
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
        let eot_mark = transaction_id_with_mark & 0b11;
        let transaction_id = transaction_id_with_mark & (!0b11);
        match eot_mark {
            0 => {
                // TODO: implement it.
                None
            }
            1 => {
                // Prepared.
                let (instant, value) = read_part::<S::Instant>(value)?;
                Some((LogRecord::Prepared(transaction_id, instant), value))
            }
            2 => {
                // Committed.
                let (instant, value) = read_part::<S::Instant>(value)?;
                Some((LogRecord::Committed(transaction_id, instant), value))
            }
            3 => {
                // Rolled back.
                let (to, value) = read_part::<u32>(value)?;
                Some((LogRecord::RolledBack(transaction_id, to), value))
            }
            _ => unreachable!(),
        }
    }

    /// Writes the data into the supplied buffer.
    ///
    /// Returns `None` if the data could not be written to the buffer, or returns the number of
    /// bytes written to the buffer.
    pub(super) fn write(&self, buffer: &mut [u8]) -> Option<usize> {
        let buffer_len = buffer.len();
        match self {
            LogRecord::Prepared(transaction_id, instant) => {
                debug_assert_eq!(transaction_id & 0b11, 0);
                let eot_mark = 1;
                let transaction_id_with_mark = transaction_id | eot_mark;
                let buffer = write_part::<TransactionID>(transaction_id_with_mark, buffer)?;
                let buffer = write_part::<S::Instant>(*instant, buffer)?;
                Some(buffer_len - buffer.len())
            }
            LogRecord::Committed(transaction_id, instant) => {
                debug_assert_eq!(transaction_id & 0b11, 0);
                let eot_mark = 2;
                let transaction_id_with_mark = transaction_id | eot_mark;
                let buffer = write_part::<TransactionID>(transaction_id_with_mark, buffer)?;
                let buffer = write_part::<S::Instant>(*instant, buffer)?;
                Some(buffer_len - buffer.len())
            }
            LogRecord::RolledBack(transaction_id, to) => {
                debug_assert_eq!(transaction_id & 0b11, 0);
                let eot_mark = 3;
                let transaction_id_with_mark = transaction_id | eot_mark;
                let buffer = write_part::<TransactionID>(transaction_id_with_mark, buffer)?;
                let buffer = write_part::<u32>(*to, buffer)?;
                Some(buffer_len - buffer.len())
            }
        }
    }
}

impl<S: Sequencer> PartialEq for LogRecord<S> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
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
    use crate::AtomicCounter;
    use quickcheck::QuickCheck;

    #[test]
    fn prop_read_write() {
        fn read_write(seed: usize) -> bool {
            let eot_mark = seed % 0b11;
            let transaction_id = (seed & (!0b11)) as u64;
            let instant = seed.rotate_left(32) as u64;
            let mut small_buffer = [0; 5];
            let mut medium_buffer = [0; 12];
            let mut large_buffer = [0; 32];

            match eot_mark {
                0 => {
                    // TODO: implement and test it.
                    true
                }
                1 => {
                    let prepared = LogRecord::<AtomicCounter>::Prepared(transaction_id, instant);
                    assert!(prepared.write(&mut small_buffer).is_none());
                    assert!(prepared.write(&mut medium_buffer).is_none());
                    assert!(prepared.write(&mut large_buffer).is_some());
                    let Some((recovered, _)) = LogRecord::<AtomicCounter>::from_raw_data(&large_buffer) else {
                        return false;
                    };
                    recovered == prepared
                }
                2 => {
                    let committed = LogRecord::<AtomicCounter>::Committed(transaction_id, instant);
                    assert!(committed.write(&mut small_buffer).is_none());
                    assert!(committed.write(&mut medium_buffer).is_none());
                    assert!(committed.write(&mut large_buffer).is_some());
                    let Some((recovered, _)) = LogRecord::<AtomicCounter>::from_raw_data(&large_buffer) else {
                        return false;
                    };
                    recovered == committed
                }
                3 => {
                    let rolled_back = LogRecord::<AtomicCounter>::RolledBack(transaction_id, 1);
                    assert!(rolled_back.write(&mut small_buffer).is_none());
                    assert!(rolled_back.write(&mut medium_buffer).is_some());
                    assert!(rolled_back.write(&mut large_buffer).is_some());
                    let Some((recovered_from_medium, _)) = LogRecord::<AtomicCounter>::from_raw_data(&medium_buffer) else {
                        return false;
                    };
                    let Some((recovered_from_large, _)) = LogRecord::<AtomicCounter>::from_raw_data(&large_buffer) else {
                        return false;
                    };
                    recovered_from_large == rolled_back && recovered_from_medium == rolled_back
                }
                _ => unreachable!(),
            }
        }
        QuickCheck::new().quickcheck(read_write as fn(usize) -> bool);
    }
}
