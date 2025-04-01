# mysqlite

A playground for learning database stuff.

The goal of this project is to build a toy SQLite clone by following the tutorial <https://cstack.github.io/db_tutorial> (C -> Rust) with [MVCC](https://en.wikipedia.org/wiki/Multiversion_concurrency_control)
and Client-Server architecture.

## Design

The internal architecture follows SQLite’s design:

<p align="center">
  <img src="https://cstack.github.io/db_tutorial/assets/images/arch2.gif"/>
</p>

## Storage

The mysqlite storage backend is organized similarly to [InnoDB](https://dev.mysql.com/doc/refman/8.4/en/innodb-storage-engine.html). Each table has its own tablespace file (.tbd), which contains a tablespace header and a sequence of fixed-size pages. Each page includes a header and a data section that stores rows. Pages are organized as B-tree nodes. The B-tree implementation is SQLite-like and follows [this tutorial](https://cstack.github.io/db_tutorial).

Each B-tree consists of multiple nodes, with each node being one page in size. The B-tree can load a page from disk or write it back by issuing commands to the pager. There are two types of nodes: leaf nodes, which store rows, and internal nodes, which store the B-tree structure.

The pager handles reading and writing pages at specific file offsets. It also maintains an in-memory cache of recently accessed pages and decides when to flush them to disk.
