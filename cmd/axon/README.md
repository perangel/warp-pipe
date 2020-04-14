# axon

_NOTE: This is still a work-in-progress._

A tool for applying warp-pipe changesets across database replicas

## Encrypting an RDS instance

1. Install `warp-pipe`
    - `go install github.com/perangel/warp-pipe/...`

2. Prepare the source database
    - `warp-pipe setup-db -H <DB_HOST> -p <DB_PORT> -U <DB_USER> -d <DB_NAME> -P <DB_PASS>`

3. Snapshot the source database
    - *NOTE:* You must run #2 before taking the snapshot

4. Copy the snapshot with encryption

5. Create a new instance from snapshot
    - see: source database for configuration

6. Run `axon`

7. Validate new database state

8. Stop `axon`
