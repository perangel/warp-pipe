# warp-pipe

**NOTE:** NOT FOR PRODUCTION USE, THIS IS A WORK IN PROGRESS

`warp-pipe` is a simple CDC tool for Postgres.

## How does it work

`warp-pipe` supports two modes, depending on the version and configuration of Postgres that you are running:

### Logical Replication (LR)

#### Requirements

- Postgres >= 9.5
- [wal2json](https://github.com/eulerto/wal2json)
- Logical replication enabled (via postgresql.conf)

You need to set up at least two parameters at postgresql.conf:

```shell
// postgresql.conf (changes require restart)
wal_level = logical
max_replication_slots = 1
```

In `LR` mode, `warp-pipe` will connect to a replication slot on your database using the `wal2json` output plugin, and emit Changesets via channel.

**NOTE:** You must set the appropriate `REPLICA IDENTITY` on your tables if you wish to expose old values in changesets. To learn more, see [replica identity](https://www.postgresql.org/docs/9.4/sql-altertable.html#SQL-CREATETABLE-REPLICA-IDENTITY).

### Audit

#### Requirements

- Postgres >= 9.4

In `audit` mode, `warp-pipe` creates a new schema (`warp_pipe`) with a `changesets` tables in your database to track modifications on your schema's tables. A `trigger` is registered with all configured tables to notify (via `NOTIFY/LISTEN`) when there are new changes to be read.

### Installation

Install the `warp-pipe` library with:

```shell
go get github.com/perangel/warp-pipe
```

Install the library and daemon with:

```shell
go get github.com/perangel/warp-pipe/...
```

## Demo

The demo shows how data in a Source DB can be accessed as a stream and used to
sync to a Target DB.

1. Creates two databases in Docker.
2. Sets up a schema on the Source DB.
3. Sets up a schema on the Target DB.
4. Runs `warp-pipe setup-db` on the Source DB.
5. Pauses to allow the user to load a front-end web application which loads Warp
   Pipe Changesets via websockets.
6. Adds data to the Source DB.
7. Runs `axon` to sync data from the Source to Target.

Run:

```bash
make demo
make demo-clean
```

### Usage

```text
Run a warp-pipe and stream changes from a Postgres database.

Usage:
  warp-pipe [flags]
  warp-pipe [command]

Available Commands:
  help        Help about any command
  setup-db    Setup the source database
  teardown-db Teardown the `warp_pipe` schema 

Flags:
      --start-from-id int              stream all changes starting from the provided changeset ID (default -1)
      --start-from-ts int              stream all changes starting from the provided timestamp (default -1)
  -M, --replication-mode string        replication mode (default "lr")
      --replication-slot-name string   replication slot name
  -i, --ignore-tables strings          tables to ignore during replication
  -w, --whitelist-tables strings       tables to include during replication
  -H, --db-host string                 database host
  -d, --db-name string                 database name
  -P, --db-pass string                 database password
  -p, --db-port int                    database port
  -U, --db-user string                 database user
  -L, --log-level string               log level (default "info")
  -h, --help                           help for warp-pipe

Use "warp-pipe [command] --help" for more information about a command.
```

### Configuration

| Flag                   | Environment Variable | Description                                                                                                    | Mode  |
| ---------------------- | -------------------- | -------------------------------------------------------------------------------------------------------------- | ----- |
| --start-from-id        | START_FROM_ID        | Sets the changeset ID from which to start relaying changesets                                                  | audit |
| --start-from-ts        | START_FROM_TIMESTAMP | Sets the timestamp from which to start replaying changesets                                                    | audit |
| -M, --replication-mode | REPLICATION_MODE     | Sets the replication mode to one of `audit` or `lr` (logical replication) (see: [requirements](#requirements)) | \*    |
| -i, --ignore-tables    | IGNORE_TABLES        | Specify tables to exclude from replication.                                                                    | \*    |
| -w, --whitelist-tables | WHITELIST_TABLES     | Specify tables to include during replication.                                                                  | \*    |
| -H, --db-host          | DB_HOST              | The database host.                                                                                             | \*    |
| -d, --db-name          | DB_NAME              | The database name.                                                                                             | \*    |
| -P, --db-pass          | DB_PASS              | The database password.                                                                                         | \*    |
| -p, --db-port          | DB_PORT              | The database port.                                                                                             | \*    |
| -U, --db-user          | DB_USER              | The database user.                                                                                             | \*    |
| -L, --log-level        | LOG_LEVEL            | Sets the logging level                                                                                         | \*    |

## Additional Reading

- https://paquier.xyz/postgresql-2/postgres-9-4-feature-highlight-replica-identity-logical-replication/ - Useful article explaining the `REPLICA IDENTITY` feature in Postgres 9.4+
