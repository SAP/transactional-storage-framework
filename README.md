<!--
SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>

SPDX-License-Identifier: Apache-2.0
-->

# Transactional Storage Framework

The transactional storage framework is a software framework that offers key operation interfaces and basic functionality for a complete transactional storage system. It is aimed at enthusiastic developers and academic researchers wanting to implement and test new transactional mechanisms on a concrete code base. It consists of multiple abstract modules as follows, and each of them allows freedom to developers to define desired semantics of actions.

* [tss::Storage](#storage)
* [tss::Container](#container)
* [tss::Sequencer](#sequencer)
* [tss::Snapshot](#snapshot)
* [tss::Transaction](#transaction)
* [tss::Logger](#logger)
* [tss::Version](#version)

This project is inspired by the paper <cite>"The tale of 1000 Cores: an evaluation of concurrency control on real(ly) large multi-socket hardware"[1]</cite>. The authors of the paper wrote a toy program to conduct a series of experiments on a large machine in order to observe hot-spots caused by the large number of processors. It turns out that small, toy programs are very useful when it comes to checking if a specific database mechanism scales well as the number of processors increases, because those adverse effects that the paper describes will be hardly detected on a large database instance running on the same hardware.

Therefore, the goal of the project is to provide a transactional storage system framework that enables developers to easily validate algorithms before applying them to a real world system.

[1]: Bang, Tiemo and May, Norman and Petrov, Ilia and Binnig, Carsten, 2020, Association for Computing Machinery

## tss::Storage <a name="storage">

tss::Storage is the main module of the whole framework that satisfies atomicity, consistency, isolation, and durability properties. It is analogous to a database in database management software. The underlying storage can either be a file or a large chunk of system memory depending on the actual implementation.

```rust
use tss::{DefaultSequencer, Storage};
let storage: Storage<DefaultSequencer> = Storage::new(String::from("db"));
```

## tss::Container <a name="container">

tss::Container is a transactional data container that is analogous to a database table in database management software. Its data is organized in accordance with the metadata embedded inside the container. Containers are hierarchically managed, and can be uniquely identified by a string.

```rust
use tss::{DefaultSequencer, Storage, Transaction};

let storage: Storage<DefaultSequencer> = Storage::new(String::from("db"));
let transaction = storage.transaction();

let result = storage.create_directory("/thomas/eats/apples", &transaction);
assert!(result.is_ok());
```

## tss::Sequencer <a name="sequencer">

tss::Sequencer is a logical clock generator that gives an identifier to each storage database change committed by a transaction. The framework allows one to implement the Lamport vector clock as the Sequence trait only assumes PartialOrd for the clock value. It offers a default sequencer that is based on a single atomic counter. It is the most important type in a transactional storage system as it defines the flow of time.

```rust
pub struct DefaultSequencer {
    clock: AtomicUsize,
    min_heap: std::sync::Mutex<std::collections::BTreeMap<usize, usize>>,
}
```

## tss::Snapshot <a name="snapshot">

tss::Snapshot represents a snapshot of a tss::Storage instance at a certain point of time represented by an identifier issued by tss:Sequencer.

```rust
use tss::{DefaultSequencer, Storage};

let storage: Storage<DefaultSequencer> = Storage::new(String::from("db"));
let mut transaction = storage.transaction();

let result = storage.create_directory("/thomas/eats/apples", &transaction);
assert!(result.is_ok());
transaction.commit();

let snapshot = storage.snapshot();
let result = storage.get("/thomas/eats/apples", &snapshot);
assert!(result.is_some());
```

## tss::Transaction <a name="transaction">

tss::Transaction represents a set of changes made to a tss::Storage that can be atomically committed. Developers and researchers are able to add / modify / remove transactional semantics easily as the storage actions are implemented in a highly flexible way. The logging format is not explicitly specified, and therefore developers can freely define the log structure, or even omit logging. The changes made in a transaction can be partially reverted by using the rewinding mechanism. Every change made in a transaction must be submitted, and the submitted change can be discarded without fully rolling back the transaction.

```rust
use tss::{DefaultSequencer, Log, Storage, Transaction};

let storage: Storage<DefaultSequencer> = Storage::new(String::from("db"));
let mut transaction = storage.transaction();
let result = transaction.rewind(1);
assert!(result.is_err());

let transaction_record = transaction.start();
transaction.submit(transaction_record);

let result = transaction.rewind(0);
assert!(result.is_ok());

transaction.commit();
```

## tss::Logger <a name="logger">
tss::Logger is an abstract module for implementing write-ahead-logging mechanisms.

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

## tss::Version <a name="version">
tss::Version is a type trait for all the versioned data that a storage instance manages. The interfaces are used by storage readers to determine if they are allowed to read the data, or by storage writers to check if they are allowed to modify the versioned object. The locking mechanism is closely tied to the versioning mechanism in this framework by default, however, it is totally up to developers to have a separate lock table without relying on the default locking mechanism.

```rust
use tss::{DefaultSequencer, DefaultVersionedObject, Storage, Transaction, Version};

let versioned_object = DefaultVersionedObject::new();
let storage: Storage<DefaultSequencer> = Storage::new(String::from("db"));
let mut transaction = storage.transaction();

let mut transaction_record = transaction.start();
assert!(transaction_record.lock(&versioned_object).is_ok());
transaction.submit(transaction_record);

transaction.commit();

let snapshot = storage.snapshot();
let guard = crossbeam_epoch::pin();
assert!(unsafe { versioned_object.version_cell(&guard).deref() }.predate(&snapshot));
```

