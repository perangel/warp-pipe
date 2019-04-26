# warp-pipe

**NOTE:** NOT FOR PRODUCTION USE, THIS IS A WORK IN PROGRESS

`warp-pipe` is a tool for streaming updates from Postgres. It uses either logical
replication or an audit table to capture changes to your data.

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

**NOTE:** You must set the appropriate `REPLICA IDENTITY` on your tables if you wish to expose old values in changesets. To learn more, see [replica identity](https://www.postgresql.org/docs/9.4/sql-altertable.html#SQL-CREATETABLE-REPLICA-IDENTITY).

### Audit

#### Requirements

* Postgres >= 9.4

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

### Usage

```text
Run a warp-pipe and stream changes from a Postgres database.

Usage:
  warp-pipe [flags]
  warp-pipe [command]

Available Commands:
  help        Help about any command
  setup-db    Setup the source database

Flags:
  -S, --db-schema string           database schema to replicate (default "public")
  -M, --replication-mode string    replication mode (default "lr")
  -i, --ignore-tables strings      tables to ignore during replication
  -w, --whitelist-tables strings   tables to include during replication
  -H, --db-host string             database host
  -d, --db-name string             database name
  -P, --db-pass string             database password
  -p, --db-port int16              database port
  -U, --db-user string             database user
  -L, --log-level string           log level (default "info")
  -h, --help                       help for warp-pipe

Use "warp-pipe [command] --help" for more information about a command.
```

### Configuration

 Flag | Environment Variable | Description
------|----------------------|------------
--log-level, -l | LOG_LEVEL | Sets the logging level
--replication-mode, -M | REPLICATION_MODE | Sets the replication mode to one of `audit` or `lr` (logical replication) (see: [requirements](#requirements))
--ignore-tables, -i | IGNORE_TABLES | Specify tables to exclude from replication.
--whitelist-tables, -w | WHITELIST_TABLES | Specify tables to include during replication.
--db-schema, -S | DB_SCHEMA | The database schema to replicate.
--db-host, -H | DB_HOST | The database host.
--db-port, -p | DB_PORT | The database port.
--db-user, -U | DB_USER | The database user.
--db-pass, -P | DB_PASS | The database password.
--db-name, -d | DB_NAME | The database name.

## Additional Reading

* https://paquier.xyz/postgresql-2/postgres-9-4-feature-highlight-replica-identity-logical-replication/ - Useful article explaining the `REPLICA IDENTITY` feature in Postgres 9.4+
