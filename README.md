<!--
SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>

SPDX-License-Identifier: Apache-2.0
-->

# Transactional Storage Framework

[![REUSE status](https://api.reuse.software/badge/github.com/SAP/transactional-storage-framework)](https://api.reuse.software/info/github.com/SAP/transactional-storage-framework)
![GitHub Workflow Status](https://img.shields.io/github/workflow/status/SAP/transactional-storage-framework/TSS)

**The project is currently work-in-progress.**
**The whole code base will undergo extensive renovation.**

The transactional storage framework is a software framework that offers key operation interfaces and basic functionality for a complete transactional storage system. It is aimed at enthusiastic developers and academic researchers wanting to implement and test new transactional mechanisms on a concrete code base. It consists of multiple abstract modules as follows, and each of them allows freedom to developers to define desired semantics of actions.

* [tss::Storage](#Storage)
* [tss::Container](#Container)
* [tss::Sequencer](#Sequencer)
* [tss::Snapshot](#Snapshot)
* [tss::Transaction](#Transaction)
* [tss::Logger](#Logger)
* [tss::Version](#Version)

This project is inspired by the paper <cite>"The tale of 1000 Cores: an evaluation of concurrency control on real(ly) large multi-socket hardware"[1]</cite>. The authors of the paper wrote a toy program to conduct a series of experiments on a large machine in order to observe hot-spots caused by the large number of processors. It turns out that small, toy programs are useful when it comes to checking if a specific database mechanism scales well as the number of processors increases, because those adverse effects that the paper describes will be hardly detected on a large database instance running on the same hardware.

Therefore, the goal of the project is to provide a transactional storage system framework that enables developers to easily validate algorithms before applying them to a real world system. Furthermore, the framework provides realization of `Container`, `Logger`, `Sequencer`, and `Version` types, thereby making the framework itself a complete transactional storage system for a relational database system.

[1]: Bang, Tiemo and May, Norman and Petrov, Ilia and Binnig, Carsten, 2020, Association for Computing Machinery

## Storage

`Storage` is the main module of the whole framework that satisfies atomicity, consistency, isolation, and durability properties. It is analogous to a database in database management software. The underlying storage can either be a file or a large chunk of system memory depending on the actual implementation.

```rust
use tss::{AtomicCounter, Storage};
let storage: Storage<AtomicCounter> = Storage::new(None);
```

## Container

`Container` is analogous to a database table in database management software. Its data is organized in accordance with the metadata embedded inside the container. Containers are hierarchically managed, and can be uniquely identified by a string.

```rust
use tss::{AtomicCounter, Container, RelationalTable, Storage};

let storage: Storage<AtomicCounter> = Storage::new(None);

let container_data = Box::new(RelationalTable::new());
let container_handle: Handle = Container::new_container(container_data);
```

### RelationalTable

**Work-in-progress.**

The framework provides a row-oriented database table container that resembles traditional database tables.

## Sequencer

`Sequencer` is a logical clock generator that gives an identifier to a set of changes made by a transaction. The framework allows one to implement the `Lamport vector clock` as the `Sequencer` trait assumes `PartialOrd` for the clock value. It is the most important type in a transactional storage system as it defines the flow of time.

```rust
pub trait Sequencer: 'static + Default {
    type Clock: Clone + Copy + Debug + Default + PartialEq + PartialOrd + Send + Sync;
    type Tracker: Clone + DeriveClock<Self::Clock>;

    fn min(&self, order: Ordering) -> Self::Clock;
    fn get(&self, order: Ordering) -> Self::Clock;
    fn issue(&self, order: Ordering) -> Self::Tracker;
    fn update(
        &self,
        new_sequence: Self::Clock,
        order: Ordering,
    ) -> Result<Self::Clock, Self::Clock>;
    fn advance(&self, order: Ordering) -> Self::Clock;
}
```

### AtomicCounter

The framework provides an atomic-counter sequencer and a mutex-protected mean-heap snapshot tracker.

## Snapshot

`Snapshot` represents a snapshot of a `Storage` instance at a certain point of time represented by an identifier issued by `Sequencer`.

```rust
use std::time::Duration;
use tss::{AtomicCounter, Storage};

let storage: Storage<AtomicCounter> = Storage::new(None);
let transaction = storage.transaction();

let snapshot = transaction.snapshot();
let mut journal = transaction.start();
let result =
    storage.create_directory(
        "/thomas/eats/apples",
        &snapshot,
        &mut journal,
        Some(Duration::from_millis(100)));
assert!(result.is_ok());
journal.submit();
drop(snapshot);

transaction.commit();

let snapshot = storage.snapshot();
let result = storage.get("/thomas/eats/apples", &snapshot);
assert!(result.is_some());
```

## Transaction

`Transaction` represents a set of changes made to a `Storage` that can be atomically committed. Developers and researchers are able to add / modify / remove transactional semantics easily as the storage actions are implemented in a highly flexible way. The logging format is not explicitly specified, and therefore developers can freely define the log structure, or even omit logging. The database changes made carried out by a transaction can be partially reverted by using the rewinding mechanism. Every change made in a transaction must be submitted, and the submitted change can be discarded without fully rolling back the transaction.

```rust
use tss::{AtomicCounter, Storage};

let storage: Storage<AtomicCounter> = Storage::new(None);
let storage_snapshot = storage.snapshot();

let transaction = storage.transaction();
let transaction_snapshot = transaction.snapshot();
let mut journal = transaction.start();
assert!(storage
    .create_directory("/thomas/eats/apples", &transaction_snapshot, &mut journal, None)
    .is_ok());

// journal_snapshot includes changes pending in the journal.
let journal_snapshot = journal.snapshot();
assert!(storage
    .get("/thomas/eats/apples", &journal_snapshot)
    .is_some());
drop(journal_snapshot);
journal.submit();

// storage_snapshot had been taken before the transaction started.
assert!(storage
    .get("/thomas/eats/apples", &storage_snapshot)
    .is_none());
// transaction_snapshot had been taken before the journal started.
assert!(storage
    .get("/thomas/eats/apples", &transaction_snapshot)
    .is_none());

let storage_snapshot = storage.snapshot();

drop(transaction_snapshot);
assert!(transaction.commit().is_ok());

// storage_snapshot had been taken before the transaction was committed.
assert!(storage
    .get("/thomas/eats/apples", &storage_snapshot)
    .is_none());

let storage_snapshot = storage.snapshot();

// storage_snapshot was taken after the transaction had been committed.
assert!(storage
    .get("/thomas/eats/apples", &storage_snapshot)
    .is_some());
```

## Logger

`Logger` is an abstract module for implementing write-ahead-logging mechanisms.

```rust
pub trait Logger<S: Sequencer> {
    fn new(anchor: &str) -> Self;
    fn submit(
        &self,
        log_data: Vec<u8>,
        transaction: &Transaction<S>,
    ) -> Result<(usize, usize), Error>;
    fn persist(&self, position: usize) -> Result<usize, Error>;
    fn recover(&self, until: Option<S::Clock>) -> Option<ContainerHandle<S>>;
}
```

### FileLogger

**Work-in-progress.**

The framework provides a file-based logger that is capable of lock-free check-pointing and point-in-time recovery.

## Version

`Version` is a type trait for all the versioned data that a storage instance manages. The interfaces are used by storage readers to determine if they are allowed to read the data, or by storage writers to check if they are allowed to create the versioned object. The locking mechanism is closely tied to the versioning mechanism in this framework by default, however, it is totally up to developers to have a separate lock table without relying on the default locking mechanism. The details of the versioning mechanism are described in the following section.

### Locking and versioning semantics

`Transaction` and `Version` provides a rudimentary, yet versatile locking and versioning mechanism. A versioned database object can only be created once, and it can never be modified after the transaction having created the versioned object is committed. In order to make update actions on a single record serializable, an empty versioned object is created, making it reachable, and then it lets transactions compete to own it. To be specific, once a versioned object is initially created, it becomes globally reachable before being filled with contents. There can be multiple transactions attempting to own the versioned object, and the first one who acquires the lock of the version among those that are able to see the previous version has a chance to fill the version with contents. If the transaction is rolled back, the chance is passed to the next transaction queued in the wait queue of the transaction, otherwise, the versioned object is marked with the transaction's commit snapshot time point value. The semantics is slightly different from conventional database systems, but it serves most types of workloads without a problem, because waiting for a lock usually results in serialization failure errors in conventional database systems. The details are as follows.

* INSERT/UPDATE/DELETE. It is simple; install an empty `Version`, and then update the contents afterwards.

* SELECT FOR UPDATE. Similar to INSERT/UPDATE/DELETE, though very inefficient as it requires dummy version creation.

* SELECT FOR SHARE. `Version` does not support shared-locking. Developers may need to implement a separate lock table.

```rust
use tss::{AtomicCounter, RecordVersion, Storage, Version};

let versioned_object: RecordVersion<usize> = RecordVersion::default();
let storage: Storage<AtomicCounter> = Storage::new(None);

let mut transaction = storage.transaction();
let mut journal = transaction.start();
assert!(journal.create(
    &versioned_object,
    |d| {
        *d = 1;
        Ok(None)
    },
    Some(Duration::from_millis(100))).is_ok());
journal.submit();
transaction.commit();

let snapshot = storage.snapshot();
assert!(versioned_object.predate(&snapshot, &scc::ebr::Barrier::new()));
```

### RecordVersion

**Work-in-progress.**

The framework provides a traditional record-level versioning mechanism for `RelationalTable`.
