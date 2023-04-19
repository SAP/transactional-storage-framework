<!--
SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>

SPDX-License-Identifier: Apache-2.0
-->

# Architecture

## Control flow

### Insert evanescent data into the database

* Acquire exclusive locks on newly created data

* Generate log records

* Commit the transaction

* Rollback the transaction

### Read data from the database

* Determine the database snapshot for the read operation

Each individual read operation needs to have a consistent view on the database, which is also called _a database snapshot_. A database snapshot is represented by a logical time point in the database system at which the database snapshot was created, and readers get time point values before they start reading the database. The time point values used by readers are monitored by the database system in order for the MVCC garbage collector to calculate the oldest used logical time point.

* Determine the target container

### Delete data from the database

* Acquire exclusive locks on target data

As soon as target data is evaluated, the target data is locked for deletion through the access controller.

* Generate log records

* Commit the transaction

* Rollback the transaction

### MVCC Garbage Collection

* Deleted data

The access controller keeps access control data of deleted data until the data becomes completely unreachable. The access controller notifies the owner of the deleted data that it is going to delete the corresponding access control data before actually deleting it, so that the owner is able to unlink all the possible paths to the data; here, the ordering between the access controller manipulation and unlinking must be kept since transactions check the access control data first, and then if the data is still reachable afterwards.

### Generate a checkpoint
