<!--
SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>

SPDX-License-Identifier: Apache-2.0
-->

# Transactional Storage Framework

[![REUSE status](https://api.reuse.software/badge/github.com/SAP/transactional-storage-framework)](https://api.reuse.software/info/github.com/SAP/transactional-storage-framework)
![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/SAP/transactional-storage-framework/tss.yml?branch=main)

**The project is currently work-in-progress.**
**The whole code base is currently undergoing extensive renovation.**

`Transactional Storage Framework` is a software framework providing methods and type traits for a complete transactional storage system. It is aimed at enthusiastic developers and academic researchers wanting to implement and test new transactional mechanisms on a concrete code base. It consists of multiple abstract modules as follows, and each of them allows freedom to developers to define desired semantics of actions.

* [tss::Database](#Database)
* [tss::Container](#Container)
* [tss::Sequencer](#Sequencer)
* [tss::Snapshot](#Snapshot)
* [tss::Transaction](#Transaction)
* [tss::Logger](#Logger)
* [tss::LockTable](#LockTable)

This project is inspired by the paper <cite>"The tale of 1000 Cores: an evaluation of concurrency control on real(ly) large multi-socket hardware"[1]</cite>. The authors of the paper wrote a toy program to conduct a series of experiments on a large machine in order to observe hot-spots caused by the large number of processors. It turns out that small, toy programs are useful for scalability testing of a database operation mechanism, because commercial full-fledged database systems usually do not provide a fully isolated environment, thus making it harder to clearly spot any performance bottleneck. `Transactional Storage Framework` is a modular system that allows any developers or researchers to activate only relevant components in the system to help them test any new innovative methods for a database system.

[1]: Bang, Tiemo and May, Norman and Petrov, Ilia and Binnig, Carsten, 2020, Association for Computing Machinery

## Database

`Database` is the main module of the whole framework.

```rust
use tss::Database;

let database = Database::default();
```

## Container

`Container` is analogous to a database table in database management software. Its data is organized in accordance with the metadata embedded inside the container. Containers are hierarchically managed, and can be uniquely identified by a string.

```rust
use tss::Database;

let database = Database::default();

let name = "hello".to_string();
let metadata = Metadata::default();
let transaction = database.transaction();
let mut journal = transaction.start();
let container_handle = database.create_container(name, metadata, &mut journal, None).await;
```

## Transaction

`Transaction` represents a set of changes made to a `Database` that can be atomically committed. Developers and researchers are able to add / modify / remove transactional semantics easily as the database actions are implemented in a highly flexible way. The logging format is not explicitly specified, and therefore developers can freely define the log structure, or even omit logging. The database changes made carried out by a transaction can be partially reverted by using the rewinding mechanism. Every change made in a transaction must be submitted, and the submitted change can be discarded without fully rolling back the transaction.

```rust
use tss::Database;

let database = Database::default();
let database_snapshot = database.snapshot();

let transaction = database.transaction();
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
assert!(transaction.commit().await.is_ok());

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

## LockTable

`LockTable` is a map expressing the state of a record.

## [Changelog](https://github.com/SAP/transactional-storage-framework/blob/main/CHANGELOG.md)
