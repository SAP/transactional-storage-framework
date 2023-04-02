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

## Database

`Database` is the main module of the whole framework, and it cannot be replaced with a customized module.

```rust
use sap_tsf::Database;

let database = Database::default();
```

## Container

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

## Sequencer

`Sequencer` defines the logical flow of time in `Database`. The default `Sequencer` is based on an atomic integer counter, however it is free to install a new customized `Sequencer` module, e.g., an implementation of `Vector Clock`, as long as the generated values are partially ordered.

## Snapshot

`Snapshot` is not a replaceable module, but the implementation is highly dependent on the `Sequencer` module.

## Transaction

`Transaction` represents a set of changes made to a `Database` that can be atomically committed. The module cannot be replaced with a new one, however developers are able to add / modify / remove transactional semantics easily since the interface and code are simple enough to understand.

```rust
use sap_tsf::Database;

let database = Database::default();
let database_snapshot = database.snapshot();

let transaction = database.transaction();
let transaction_snapshot = transaction.snapshot();
let journal = transaction.start();
```

## AccessController

`AccessController` maps a database object onto the current state of it; using the information, `AccessController` can tell the transaction if it can read or modify the database object.

## PersistenceLayer

`PersistenceLayer` is an abstract module for implementing write-ahead-logging mechanisms and point-in-time-recovery.

## [Changelog](https://github.com/SAP/transactional-storage-framework/blob/main/CHANGELOG.md)
