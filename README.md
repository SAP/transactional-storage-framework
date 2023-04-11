<!--
SPDX-FileCopyrightText: 2021 Changgyoo Park <wvwwvwwv@me.com>

SPDX-License-Identifier: Apache-2.0
-->

# Transactional Storage Framework

[![REUSE status](https://api.reuse.software/badge/github.com/SAP/transactional-storage-framework)](https://api.reuse.software/info/github.com/SAP/transactional-storage-framework)
![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/SAP/transactional-storage-framework/sap_tsf.yml?branch=main)

**The project is currently work-in-progress.**

**The whole code base is currently undergoing extensive renovation.**

`Transactional Storage Framework` is a software framework providing methods and type traits for a complete transactional storage system. It is aimed at enthusiastic developers and academic researchers wanting to implement and test new transactional mechanisms on a concrete code base. It consists of multiple abstract modules as follows, and several of them allow developers to define desired semantics of actions.

* [sap_tsf::Database](#Database)
* [sap_tsf::Container](#Container)
* [sap_tsf::Sequencer](#Sequencer)
* [sap_tsf::Snapshot](#Snapshot)
* [sap_tsf::Transaction](#Transaction)
* [sap_tsf::AccessController](#AccessController)
* [sap_tsf::PersistenceLayer](#PersistenceLayer)

This project is inspired by the paper <cite>"The tale of 1000 Cores: an evaluation of concurrency control on real(ly) large multi-socket hardware"[1]</cite>. The authors of the paper wrote a toy program to conduct a series of experiments on a large machine in order to observe hot-spots caused by the large number of processors. It turns out that small, toy programs are useful for scalability testing of a database operation mechanism, because commercial full-fledged database systems usually do not provide a fully isolated environment, thus making it harder for researchers to clearly spot any performance bottleneck. `Transactional Storage Framework` is a modular system that allows any developers or researchers to activate only relevant components in the system to help them test any new innovative methods for a database system.

[1]: Bang, Tiemo and May, Norman and Petrov, Ilia and Binnig, Carsten, 2020, Association for Computing Machinery

## Overview

The framework is fully written in Rust, and incorporates state-of-the-art programming techniques to maximize the performance while minimizing the memory usage.

* Asynchronous API: any code that accesses shared data is always either lock-free or asynchronous, and therefore computation resources are cooperatively scheduled.
* No dependencies on asynchronous executors: users can freely use their own asynchronous executors; one drawback is, the framework spawns a `thread` to implement a timer, however the thread will mostly lie dormant.
* Zero busy-loops: no spin-locks and busy-loops to wait for desired resources.

## Components

The framework consists of generic components that can be customizable and pluggable.

### Database

`Database` is the main module of the whole framework. `Database` comes with two type parameters: `S: Sequencer` and `P: PersistenceLayer`.

```rust
use sap_tsf::Database;

let database = Database::default();
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

### Sequencer

`Sequencer` defines the logical flow of time in `Database`. The default `Sequencer` is based on an atomic integer counter, however it is free to install a new customized `Sequencer` module, e.g., an implementation of `Vector Clock`, as long as the generated values are partially ordered.

### Snapshot

`Snapshot` is not a replaceable module, but the implementation is highly dependent on the `Sequencer` module.

### Transaction

`Transaction` represents a set of changes made to a `Database` that can be atomically committed. The module cannot be replaced with a new one, however developers are able to add / modify / remove transactional semantics easily since the interface and code are simple enough to understand.

```rust
use sap_tsf::Database;

let database = Database::default();
let database_snapshot = database.snapshot();

let transaction = database.transaction();
let transaction_snapshot = transaction.snapshot();
let journal = transaction.start();
```

### AccessController

`AccessController` maps a database object onto the current state of it; using the information, `AccessController` can tell the transaction if it can read or modify the database object. In other words, `AccessController` controls locking and versioning of database objects.

```rust

use sap_tsf::{Database, ToObjectID};

// `O` represents a database object type.
struct O(usize);


// `ToObjectID` is implemented for `O` so that `AccessController` can derive the
// identifier of a database object represented by an instace of `O`.
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

    // The transaction writes its own commit instant value onto the database object, and only
    // future readers and writers can gain access to the database object.
    assert!(transaction.commit().await.is_ok());

    let snapshot = database.snapshot();
    assert_eq!(access_controller.read(&O(1), &snapshot, None).await, Ok(true));

    let transaction_succ = database.transaction();
    let mut journal_succ = transaction_succ.journal();

    // The transaction owns the database object to prevent any other transactions to get write
    // access to it.
    assert!(access_controller.share(&O(1), &mut journal_succ, None).await.is_ok());
    assert_eq!(journal_succ.submit(), 1);

    let transaction_fail = database.transaction();
    let mut journal_fail = transaction_fail.journal();
    assert!(access_controller.lock(&O(1), &mut journal_fail, None).await.is_err());

    let mut journal_delete = transaction_succ.journal();

    // The transaction will delete the database object.
    assert!(access_controller.delete(&O(1), &mut journal_delete, None).await.is_ok());

    assert_eq!(journal_delete.submit(), 2);
 
    // The transaction deleted the database object by writing its commit instant value onto
    // the database object.
    assert!(transaction_succ.commit().await.is_ok());

    // Further access to the database object is prohibited.
    assert!(access_controller.share(&O(1), &mut journal_fail, None).await.is_err());
};
 ```

### PersistenceLayer

`PersistenceLayer` is an abstract module for implementing write-ahead-logging mechanisms and point-in-time-recovery.

## [Changelog](https://github.com/SAP/transactional-storage-framework/blob/main/CHANGELOG.md)
