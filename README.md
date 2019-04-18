# warp-pipe

**NOTE:** NOT FOR PRODUCTION USE, THIS IS A WORK IN PROGRESS

`warp-pipe` is a tool for streaming updates from Postgres.

## How does it work

`warp-pipe` supports two modes, depending on the version and configuration of Postgres that you are running:

### Logical Replication (LR)

#### Requirements

* Postgres >= 9.5
* [wal2json](https://github.com/eulerto/wal2json) 
* Logical replication enabled (via postgresql.conf)

You need to set up at least two parameters at postgresql.conf:

```shell
// postgresql.conf (changes require restart)
wal_level = logical
max_replication_slots = 1
```

In `LR` mode, `warp-pipe` will connect to a replication slot on your database using the `wal2json` output plugin, and emit Changesets via channel.

### Queue

#### Requirements

* Postgres >= 9.4

In `queue` mode, `warp-pipe` creates a set of `changesets` tables in your database to track changes. A `trigger` is registered that will notify a listener (via `NOTIFY/LISTEN`) when there are new changes to be read from the table.

### Installation

TODO

### Usage

TODO

### Configuration

TODO
