<!--
SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>

SPDX-License-Identifier: Apache-2.0
-->

# Architecture

This documentation describes how the framework and the file-IO persistence layer ensure the ACID properties and implement multi-version concurrency control.

## Control flow

### Insert data into the database

* Allocate necessary pages for the data.

* Acquire exclusive locks on newly created data.

Exclusive locks acquired for creating new data by a transaction are treated in a special way that they turn into the logical time point of the transaction commit exactly when the transaction is committed. The logical time point values which were initially exclusive locks are used for multi-version concurrency control.

* Write the data to the allocated pages with the data marked invisible.

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

As soon as target data is evaluated, the target data is locked for deletion through the access controller.

* Generate log records.

* Commit the transaction.

* Rollback the transaction.

### MVCC Garbage Collection

* Deleted data.

The access controller keeps access control data of deleted data until the data becomes completely unreachable. The access controller notifies the owner of the deleted data that it is going to delete the corresponding access control data before actually deleting it, so that the owner is able to unlink all the possible paths to the data; here, the ordering between the access controller manipulation and unlinking must be kept since transactions check the access control data first, and then if the data is still reachable afterwards.

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
