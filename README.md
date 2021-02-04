# Transactional Storage Framework

The transactional storage framework is a software framework that offers key operation interfaces and basic functionality for a complete transactional storage system. It is aimed at enthusiastic developers and academic researchers wanting to implement and test new transactional mechanisms on a concrete code base. It consists of multiple abstract modules as follows, and each of them allows freedom to developers to define desired semantics of actions.

* [tss::Storage](#storage)
* [tss::Container](#container)
* [tss::Sequencer](#sequencer)
* [tss::Snapshot](#snapshot)
* [tss::Transaction](#transaction)
* [tss::Logger](#logger)

This project is inspired by the paper <cite>"The tale of 1000 Cores: an evaluation of concurrency control on real(ly) large multi-socket hardware"[1]</cite>. The authors of the paper wrote a toy program to conduct a series of experiments on a large machine in order to observe hotspots caused by the large number of processors. It turns out that small, toy programs are very useful when it comes to checking if a specific database mechanism scales well as the number of processors increases, because those adverse effects that the paper describes will be hardly detected on a large database instance running on the same hardware.

Therefore, the goal of the project is to provide a transactional storage system framework that enables developers to easily validate algorithms before applying them to a real world system.

[1]: Bang, Tiemo and May, Norman and Petrov, Ilia and Binnig, Carsten, 2020, Association for Computing Machinery

## tss::Storage <a name="storage">

tss::Storage is the main module of the whole framework that satisfies atomicity, consistency, isolation, and durability properties. It is analogous to a database in database management software. The underlying storage can either be a file or a large chunk of system memory depending on the actual implementation.

## tss::Container <a name="container">

tss::Container is a transactional data container that is analogous to a database table in database management software. Its data is organized in accordance with the metadata embedded inside the container.

## tss::Sequencer <a name="sequencer">

tss::Sequencer is a logical clock generator that gives an identifier to each storage database change committed by a transaction. The framework allows one to implement the Lamport vector clock as the Sequence trait only assumes PartialOrd for the clock value. It offers a default sequencer that is based on a single atomic counter. It is the most important type in a transactional storage system as it defines the flow of time.

## tss::Snapshot <a name="snapshot">

tss::Snapshot represents a snapshot of the state of a tss::Storage instance at a certain point of time represented by an identifier issued by tss::Sequencer.

## tss::Transaction <a name="transaction">

tss::Transaction represents a single storage transaction. Developers and researchers are able to add / modify / remove transactional semantics easily as the storage actions are implemented in a highly flexible way. The logging format is not explicitly specified, and therefore developers can freely define the log structure, or even omit logging.

## tss::Logger <a name="logger">
tss::Logger is an abstract module for implementing write-ahead-logging mechanisms.
