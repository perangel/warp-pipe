# warp-pipe

**NOTE:** NOT FOR PRODUCTION USE, THIS IS A WORK IN PROGRESS

`warp-pipe` is a tool for replicating data from Postgresql.

## How does it work?

`warp-pipe` supports two modes, depending on the version and configuration of Postgres that you are running:

- queue (via LISTEN/NOTIFY)
  - requires pg >= 9.4
- streaming (via [logical replication](https://www.postgresql.org/docs/10/logical-replication.html) and [wal2json](https://github.com/eulerto/wal2json))
  - requires pg >= 9.5 and `wal2json` decoder

### Queue Mode

In `queue` mode, `warp-pipe` will create a table to track changes changes (similar to an audit table) and register a trigger that will NOTIFY a listener that there are changes to be read from the changeset table.

This mode is useful for when you want to start replicating changes from your database but don't run a version that supports logical replication or has `wal2json` installed.

### Streaming Mode

In `streaming` mode, `warp-pipe` will connect to a replication slot using **logical replication** and `wal2json` as the decoder.

### Configuration

In both modes `warp-pipe` can be configured to transmit changes for all tables in schema, or only a whitelisted set.

A number of optional **transformers** can be plugged in to transform the changesets into the desired downstream format. 

`warp-pipe` also supports streaming changes via gRPC, Kafka.
