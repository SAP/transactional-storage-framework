<!--
SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>

SPDX-License-Identifier: Apache-2.0
-->

# Architecture

This documentation describes how the framework and the file-IO persistence layer ensure the ACID properties and implement multi-version concurrency control.

## Persistence Layer

* WORK IN PROGRESS.

The default persistence layer implementation heavily relies on [io_uring](https://en.wikipedia.org/wiki/Io_uring) which is unfortunately only available on `Linux`. The details can be found in [file_io](file_io.md).

## Control Flow

### Insert data into the database

* Allocate necessary pages for the data.

* Acquire exclusive locks on newly created data.

Exclusive locks acquired for creating new data by a transaction are treated in a special way that they turn into the logical time point of the transaction commit exactly when the transaction is committed. The logical time point values which were initially exclusive locks are used for multi-version concurrency control.

* Write the data to the allocated pages.

* Generate log records containing the offsets of the data in the database file.

* Prepare the transaction for commit by ensuring that the data and log records were synchronized to the disk.

* Commit the transaction by ensuring that a commit log record for the transaction is written to the log file.

* The background task processor eventually makes the data visible and truncates related log records.

The access controller still holds necessary information to provide correct visibility of the data to readers for multi-version concurrency control.

* The background task processor eventually removes entries corresponding to the data in the access controller if the data can be observed by all the current and future readers.

### Read data from the database

* Determine the database snapshot for the read operation.

Each individual read operation needs to have a consistent view on the database, which is also called _a database snapshot_. A database snapshot is represented by a logical time point in the database system at which the database snapshot was created, and readers get time point values before they start reading the database. The time point values used by readers are monitored by the database system in order for the MVCC garbage collector to calculate the oldest used logical time point.

* Determine the target container.

### Delete data from the database

* Acquire exclusive locks on target data.

As soon as target data is evaluated, the target data is locked for deletion through the access controller. The deletion is atomically completed when the transaction is committed.

* Generate log records.

Log records containing the data identifiers are generated and persisted in the disk.

* Mark the target data deleted.

The corresponding data is marked deleted in the persistence layer.

* Commit the transaction.

* Garbage collect the access controller.

The access controller entries are garbage collected if all the current and future readers see the deletion.

* Deallocate pages.

Pages which only contain deleted data without associated access controller entries are pushed into the free page pool.

### MVCC Garbage Collection

* Deleted data.

* Dangling data.

The framework never generates undo log records, and the MVCC garbage collector is responsible for cleaning up of data related to rolled back transactions.

### Generate a checkpoint

* Switch the log file.

* Allocate special pages for log records of open transactions.

* Clean up old open transaction log records.

* Truncate log records for ended transactions.

* Store remaining log records in the special pages.

* Iterate over dirty pages to flush them to the disk.

* Synchronize to the disk.

* Truncate the old log file.

### Recovery

* Recover container directory.

* Apply redo log entries.
