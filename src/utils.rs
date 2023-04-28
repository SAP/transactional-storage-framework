// SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>
//
// SPDX-License-Identifier: Apache-2.0

//! Collection of utility functions.

use std::convert::Into;
use std::fmt::{self, Debug};
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::sync::{Condvar, Mutex};
use std::thread::{available_parallelism, current, ThreadId};

/// A non-cryptographic [`Hasher`] for integer values.
#[derive(Clone, Copy, Debug)]
pub struct IntHasher(ArrayOrU64);

/// A simple implementation of a binary semaphore.
#[derive(Debug, Default)]
pub struct BinarySemaphore<T: Debug + Default> {
    condvar: Condvar,
    mutex: Mutex<T>,
}

/// Returns the current thread identifier.
///
/// It caches the thread identifier in it that makes this function significantly faster than
/// [`std::thread::Thread::id`].
///
/// # Examples
///
/// ```
/// use sap_tsf::utils;
///
/// assert_eq!(utils::thread_id(), std::thread::current().id());
/// ````
#[inline]
#[must_use]
pub fn thread_id() -> ThreadId {
    THREAD_ID.with(|id| *id)
}

/// Returns the shard identifier.
///
/// A shard identifier is basically the hash value of the current thread identifier. Different
/// threads can have the same shard identifier.
///
/// # Examples
///
/// ```
/// use sap_tsf::utils;
///
/// let shard_id = utils::shard_id();
/// ````
#[inline]
#[must_use]
pub fn shard_id() -> usize {
    let thread_id = thread_id();
    let mut hasher = IntHasher::default();
    thread_id.hash(&mut hasher);
    {
        #![allow(clippy::cast_possible_truncation)]
        hasher.finish() as usize
    }
}

/// Returns the suggested number of shards.
///
/// Returns a non-zero `usize` value that is close to [`std::thread::available_parallelism`].
///
/// # Examples
///
/// ```
/// use sap_tsf::utils;
///
/// assert_ne!(utils::advise_num_shards(), 0);
/// ````
#[inline]
#[must_use]
pub fn advise_num_shards() -> usize {
    available_parallelism().ok().map_or(1, Into::into)
}

/// [`ArrayOrU64`] is a `u64` integer that can also be used as an array of `u8`.
#[derive(Clone, Copy)]
union ArrayOrU64 {
    /// Array.
    array: [u8; 8],

    /// Integer.
    integer: u64,
}

impl Default for IntHasher {
    #[inline]
    fn default() -> Self {
        Self(ArrayOrU64 { integer: 0 })
    }
}

impl Hasher for IntHasher {
    #[inline]
    fn write(&mut self, msg: &[u8]) {
        let mut iter = msg.chunks_exact(8);
        for c in iter.by_ref() {
            // Safety: the length of the array equals to that of `u64`.
            (0..8).for_each(|i| unsafe {
                self.0.array[i] ^= c[i];
            });
        }
        for i in 0..iter.remainder().len() {
            // Safety: the length of the array equals to that of `u64`.
            unsafe {
                self.0.array[i] ^= iter.remainder()[i];
            }
        }
    }

    /// The finalizer was excerpted from
    /// `http://mostlymangling.blogspot.com/2018/07/on-mixing-functions-in-fast-splittable.html`
    #[inline]
    fn finish(&self) -> u64 {
        // Safety: the length of the array equals to that of `u64`.
        let mut v = unsafe { self.0.integer };
        v ^= v.rotate_right(49) ^ v.rotate_left(24);
        v = v.wrapping_mul(0x9FB2_1C65_1E98_DF25);
        v ^= v.wrapping_shr(24);
        v = v.wrapping_mul(0x9FB2_1C65_1E98_DF25);
        v ^ v.wrapping_shr(24)
    }
}

impl<T: Debug + Default> BinarySemaphore<T> {
    /// Waits for a signal.
    #[inline]
    pub fn wait_while<F: FnMut(&mut T) -> bool>(&self, condition: F) {
        if let Ok(guard) = self.mutex.lock() {
            drop(self.condvar.wait_while(guard, condition));
        }
    }

    /// Sends a signal.
    #[inline]
    pub fn signal<F: FnOnce(&mut T)>(&self, prepare: F) {
        if let Ok(mut guard) = self.mutex.lock() {
            prepare(&mut *guard);
            self.condvar.notify_one();
        }
    }
}

impl<T: Debug + Default> Deref for BinarySemaphore<T> {
    type Target = Mutex<T>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.mutex
    }
}

impl Debug for ArrayOrU64 {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Safety: the length of the array equals to that of `u64`.
        unsafe { self.integer.fmt(f) }
    }
}

thread_local! {
    static THREAD_ID: ThreadId = current().id();
}
