<!--
SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>

SPDX-License-Identifier: Apache-2.0
-->

# Transactional Storage Framework

[![REUSE status](https://api.reuse.software/badge/github.com/SAP/transactional-storage-framework)](https://api.reuse.software/info/github.com/SAP/transactional-storage-framework)
![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/SAP/transactional-storage-framework/sap_tsf.yml?branch=main)

**The project is currently work-in-progress.**
**The whole code base is currently undergoing extensive renovation.**
**The code will be ready for use in 0.3.0.**

`Transactional Storage Framework` is a software framework providing methods and type traits for a complete transactional storage system. It is aimed at enthusiastic developers and academic researchers wanting to implement and test new transactional mechanisms on a concrete code base. It consists of multiple abstract modules as follows, and several of them allow developers to define desired semantics of actions.

* [sap_tsf::Database](#Database)
* [sap_tsf::Sequencer](#Sequencer)
* [sap_tsf::Snapshot](#Snapshot)
* [sap_tsf::Transaction](#Transaction)
* [sap_tsf::AccessController](#AccessController)
* [sap_tsf::PersistenceLayer](#PersistenceLayer)
* [sap_tsf::Container](#Container)
* [sap_tsf::Telemetry](#Telemetry)

This project is inspired by the paper <cite>"The tale of 1000 Cores: an evaluation of concurrency control on real(ly) large multi-socket hardware"[1]</cite>. The authors of the paper wrote a toy program to conduct a series of experiments on a large machine in order to observe hot-spots caused by the large number of processors. It turns out that small, toy programs are useful for scalability testing of a database operation mechanism, because commercial full-fledged database systems usually do not provide a fully isolated environment, thus making it harder for researchers to clearly spot any performance bottleneck. `Transactional Storage Framework` is a modular system that allows any developers or researchers to activate only relevant components in the system to help them test any new innovative methods for a database system.

[1]: Bang, Tiemo and May, Norman and Petrov, Ilia and Binnig, Carsten, 2020, Association for Computing Machinery

## Overview

The framework is fully written in Rust, and incorporates state-of-the-art programming techniques to maximize the performance while minimizing the memory usage.

* Asynchronous API: any code that accesses shared data is always either lock-free or asynchronous, and therefore computation resources are cooperatively scheduled.
* No dependencies on asynchronous executors: users can freely use their own asynchronous executors; one drawback is, the framework spawns a `thread` to implement a timer, however the thread will mostly lie dormant.
* Zero busy-loops: no spin-locks and busy-loops to wait for desired resources.

## Architecture

The detailed architecture of the framework is described in [Architecture](https://github.com/SAP/transactional-storage-framework/blob/main/doc/architecture.md).

## Components

The framework consists of generic components that can be customizable and pluggable.

### Database

`Database` is the main module of the whole framework. `Database` comes with two type parameters: `S: Sequencer` and `P: PersistenceLayer`.

```rust
use sap_tsf::Database;

let database = Database::default();
```

### Sequencer

`Sequencer` defines the logical flow of time in `Database`. The default `Sequencer` is based on an atomic integer counter, however it is free to install a new customized `Sequencer` module, e.g., an implementation of `Vector Clock`, as long as the generated values are partially ordered.

### Snapshot

`Snapshot` is not a replaceable module, but the implementation is highly dependent on the `Sequencer` module. A `Snapshot` represents a database state at an instant, providing a consistent view on the database.

### Transaction

`Transaction` represents a set of changes to a `Database` that can be atomically committed or rolled back. The module cannot be replaced with a new one, however developers are able to add / modify / remove transactional semantics easily since the interface and code are simple enough to understand.

```rust
use sap_tsf::Database;

let database = Database::default();

let mut transaction = database.transaction();

// `Journal` is analogous to a sub-transaction.
let journal_1 = transaction.journal();

// A `Journal` cannot be shared among tasks, but multiple `Journal` instances can be created.
let journal_2 = transaction.journal();
let journal_3 = transaction.journal();

// A `Journal` can be submitted individually, and each submitted `Journal` is sequenced.
assert_eq!(journal_1.submit(), 1);
assert_eq!(journal_3.submit(), 2);

// Drop a `Journal` if database modification carried out by the `Journal` needs to be rolled back.
drop(journal_2);

// It is possible to roll back the transaction to a certain point.
//
// Rewinding the transaction to `1` rolls back `journal_3`.
transaction.rewind(1);
```

### AccessController

`AccessController` maps a database object onto the current state of it; using the information, `AccessController` can tell the transaction if it can read or modify the database object. In other words, `AccessController` controls locking and versioning of database objects.

```rust

use sap_tsf::{Database, ToObjectID};

// `O` represents a database object type.
struct O(usize);


// `ToObjectID` is implemented for `O`.
impl ToObjectID for O {
    fn to_object_id(&self) -> usize {
        self.0
    }
}

let database = Database::default();
let access_controller = database.access_controller();

async {
    let transaction = database.transaction();
    let mut journal = transaction.journal();

    // Let the `Database` know that the database object will be created.
    assert!(access_controller.create(&O(1), &mut journal, None).await.is_ok());

    journal.submit();

    // The transaction writes its own commit instant value onto the access data of the database
    // object, and future readers and writers will be able to gain access to the database object.
    assert!(transaction.commit().await.is_ok());

    let snapshot = database.snapshot();
    assert_eq!(access_controller.read(&O(1), &snapshot, None).await, Ok(true));

    let transaction_succ = database.transaction();
    let mut journal_succ = transaction_succ.journal();

    // The transaction will own database object to prevent any other transactions from gaining
    // write access to it.
    assert!(access_controller.share(&O(1), &mut journal_succ, None).await.is_ok());
    assert_eq!(journal_succ.submit(), 1);

    let transaction_fail = database.transaction();
    let mut journal_fail = transaction_fail.journal();

    // Another transaction fails to take ownership of the database object.
    assert!(access_controller.lock(&O(1), &mut journal_fail, None).await.is_err());

    let mut journal_delete = transaction_succ.journal();

    // The transaction will delete the database object.
    assert!(access_controller.delete(&O(1), &mut journal_delete, None).await.is_ok());
    assert_eq!(journal_delete.submit(), 2);

    // The transaction deletes the database object by writing its commit instant value onto
    // the access data associated with the database object.
    assert!(transaction_succ.commit().await.is_ok());

    // Further access to the database object is prohibited.
    assert!(access_controller.share(&O(1), &mut journal_fail, None).await.is_err());
};
 ```

### Container

`Container` is analogous to a database table in database management software. Its data is organized in accordance with the metadata embedded inside the container. Containers are hierarchically managed, and can be uniquely identified by a string. The layout of a `Container` can be customized via the associated `Metadata`.

```rust
use sap_tsf::Database;

let database = Database::default();

let name = "hello".to_string();
let metadata = Metadata::default();
let transaction = database.transaction();
let mut journal = transaction.start();
let container_handle = database.create_container(name, metadata, &mut journal, None).await;
```

### PersistenceLayer

`PersistenceLayer` is an abstract module for implementing write-ahead-logging mechanisms and point-in-time-recovery.

### Telemetry

The `Telemetry` module provides monitoring tools to see the internal state of the transactional storage system and get key statistics data.

## [Changelog](https://github.com/SAP/transactional-storage-framework/blob/main/CHANGELOG.md)
