package warppipe

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"reflect"
	"strings"
	"syscall"

	"github.com/jackc/pgx"
	"github.com/jmoiron/sqlx"
	"github.com/kelseyhightower/envconfig"
	"github.com/perangel/warp-pipe/db"
	"github.com/perangel/warp-pipe/internal/store"
	"github.com/sirupsen/logrus"
)

func getDBConnString(host string, port int, name, user, pass string) string {
	return fmt.Sprintf("user=%s password=%s dbname=%s host=%s port=%d sslmode=%s",
		user,
		pass,
		name,
		host,
		port,
		"disable",
	)
}

// Axon listens for Warp-Pipe change sets events. Then converts them into SQL statements, executing
// them on the remote target.
type Axon struct {
	Config     *AxonConfig
	Logger     *logrus.Logger
	shutdownCh chan os.Signal
}

// NewAxonConfigFromEnv loads the Axon configuration from environment variables.
func NewAxonConfigFromEnv() (*AxonConfig, error) {
	config := AxonConfig{}
	err := envconfig.Process("axon", &config)
	if err != nil {
		return nil, fmt.Errorf("failed to process environment config: %w", err)
	}
	return &config, nil
}

// Run the Axon worker.
func (a *Axon) Run() error {
	if a.shutdownCh == nil {
		a.shutdownCh = make(chan os.Signal, 1)
	}

	signal.Notify(a.shutdownCh, os.Interrupt, syscall.SIGTERM)

	if a.Logger == nil {
		a.Logger = logrus.New()
		a.Logger.SetFormatter(&logrus.JSONFormatter{})
	}

	// TODO: Refactor to use just one connection to the sourceDB
	sourceDBConn, err := sqlx.Open("postgres", getDBConnString(
		a.Config.SourceDBHost,
		a.Config.SourceDBPort,
		a.Config.SourceDBName,
		a.Config.SourceDBUser,
		a.Config.SourceDBPass,
	))
	if err != nil {
		return fmt.Errorf("unable to connect to source database: %w", err)
	}

	targetDBConn, err := sqlx.Open("postgres", getDBConnString(
		a.Config.TargetDBHost,
		a.Config.TargetDBPort,
		a.Config.TargetDBName,
		a.Config.TargetDBUser,
		a.Config.TargetDBPass,
	))
	if err != nil {
		return fmt.Errorf("unable to connect to target database: %w", err)
	}

	err = checkTargetVersion(targetDBConn)
	if err != nil {
		return fmt.Errorf("unable to check target database version: %w", err)
	}

	// TODO: (1) add support for selecting the warp-pipe mode
	// TODO: (2) only print the source stats if that is audit
	sourceCount, err := printStats(sourceDBConn, "source")
	if err != nil {
		return fmt.Errorf("unable to get source db stats: %w", err)
	}
	targetCount, err := printStats(targetDBConn, "target")
	if err != nil {
		return fmt.Errorf("unable to get target db stats: %w", err)
	}

	if sourceCount == targetCount {
		a.Logger.Info("changeset counts match")
		return nil
	}

	err = loadPrimaryKeys(targetDBConn)
	if err != nil {
		return fmt.Errorf("unable to load target DB primary keys: %w", err)
	}

	err = loadColumnSequences(targetDBConn)
	if err != nil {
		return fmt.Errorf("unable to load target DB column sequences: %w", err)
	}

	err = loadOrphanSequences(sourceDBConn)
	if err != nil {
		return fmt.Errorf("unable to load source DB orphan sequences: %w", err)
	}

	// columnTypes need to be explicitly loaded for Notify Listener
	err = loadColumnTypes(sourceDBConn)
	if err != nil {
		return fmt.Errorf("unable to load column data types: %w", err)
	}

	// Create a notify listener and start from the configured changeset id.
	listener := NewNotifyListener(StartFromID(a.Config.StartFromID))

	connConfig := pgx.ConnConfig{
		Host:     a.Config.SourceDBHost,
		Port:     uint16(a.Config.SourceDBPort),
		User:     a.Config.SourceDBUser,
		Password: a.Config.SourceDBPass,
		Database: a.Config.SourceDBName,
	}

	wp, err := NewWarpPipe(&connConfig, listener)
	if err != nil {
		return fmt.Errorf("failed to establish a warp-pipe: %w", err)
	}

	err = wp.Open()
	if err != nil {
		return fmt.Errorf("failed to dial the listener: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	changes, errs := wp.ListenForChanges(ctx)

	for {
		select {
		case <-a.shutdownCh:
			a.Logger.Error("shutting down...")
			cancel()
			wp.Close()
			sourceDBConn.Close()
			targetDBConn.Close()
			return nil
		case err := <-errs:
			return fmt.Errorf("listener received an error: %w", err)
		case change := <-changes:

			// Update the column types since it is not available when using notify listener
			err = setColumnTypes(change)
			if err != nil {
				return fmt.Errorf("could not set column type for change - %s: %w", change, err)
			}

			// Override the schema if a target database schema has been configured.
			if a.Config.TargetDBSchema != "" {
				change.Schema = a.Config.TargetDBSchema
			}

			processErr := a.processChange(sourceDBConn, targetDBConn, change)
			if processErr != nil {
				return fmt.Errorf("failed to apply changeset: %s, %w", change, processErr)
			}

			if a.Config.ShutdownAfterLastChangeset {
				isLatest, err := wp.IsLatestChangeSet(change.ID)
				if err != nil {
					return fmt.Errorf("failed to determine if the sync is complete: %w", err)
				}
				if isLatest {
					a.Logger.
						WithField("component", "warp_pipe").
						Info("sync is complete. shutting down...")
					a.Shutdown()
				}
			}
		}
	}
}

func (a *Axon) getDB() (sourceDBConn *pgx.Conn, targetDBConn *pgx.Conn, err error) {

	sourceDBConn, err = pgx.Connect(pgx.ConnConfig{
		Host:     a.Config.SourceDBHost,
		Port:     uint16(a.Config.SourceDBPort),
		User:     a.Config.SourceDBUser,
		Password: a.Config.SourceDBPass,
		Database: a.Config.SourceDBName,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("unable to connect to source database: %w", err)
	}

	targetDBConn, err = pgx.Connect(pgx.ConnConfig{
		Host:     a.Config.TargetDBHost,
		Port:     uint16(a.Config.TargetDBPort),
		User:     a.Config.TargetDBUser,
		Password: a.Config.TargetDBPass,
		Database: a.Config.TargetDBName,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("unable to connect to target database: %w", err)
	}
	return
}

func (a *Axon) Verify(schemas, includeTables, excludeTables []string) error {
	sourceDBConn, targetDBConn, err := a.getDB()
	if err != nil {
		return fmt.Errorf("cannot connect to DB: %w", err)
	}
	defer sourceDBConn.Close()
	defer targetDBConn.Close()

	err = db.PrepareForDataIntegrityChecks(sourceDBConn)
	if err != nil {
		return fmt.Errorf("unable to prepare source database for Integrity checks: %w", err)
	}

	err = db.PrepareForDataIntegrityChecks(targetDBConn)
	if err != nil {
		return fmt.Errorf("unable to prepare target database for Integrity checks: %w", err)
	}

	tables, err := db.GenerateTablesList(sourceDBConn, schemas, includeTables, excludeTables)
	if err != nil {
		return fmt.Errorf("unable to generate the list of source tables to check: %w", err)
	}

	for _, table := range tables {

		a.Logger.
			WithField("table", fmt.Sprintf(`"%s"."%s"`, table.Schema, table.Name)).
			Info("Verifying checksum")

		if len(table.PKeyFields) < 1 {
			return fmt.Errorf(`table "%s"."%s" has no primary key, cannot guarantee checksum match`, table.Schema, table.Name)
		}

		orderByClause := ""
		pkColumns := make([]string, len(table.PKeyFields))
		for position, column := range table.PKeyFields {
			pkColumns[position-1] = fmt.Sprintf(`"%s"."%s"."%s"`, table.Schema, table.Name, column)
		}
		orderByClause = fmt.Sprintf(`ORDER BY %s`, strings.Join(pkColumns, ","))

		sql := fmt.Sprintf(
			`SELECT pg_md5_hashagg(md5(CAST(("%s"."%s".*)AS TEXT))%s) FROM "%s"."%s"`,
			table.Schema,
			table.Name,
			orderByClause,
			table.Schema,
			table.Name,
		)

		sourceChecksum := ""
		row := sourceDBConn.QueryRow(sql)
		err := row.Scan(&sourceChecksum)
		if err != nil {
			return fmt.Errorf("failed to scan the source checksum for table %s.%s: %w", table.Schema, table.Name, err)
		}

		targetChecksum := ""
		row = targetDBConn.QueryRow(sql)
		err = row.Scan(&targetChecksum)
		if err != nil {
			return fmt.Errorf("failed to scan the target checksum for table %s.%s: %w", table.Schema, table.Name, err)
		}

		if sourceChecksum != targetChecksum {

			return fmt.Errorf("checksums differ, source: %s target: %s", sourceChecksum, targetChecksum)
		}
	}
	return nil
}

func (a *Axon) VerifyChangesets(lastID int64) error {
	a.Logger.Info("beginning verbose check")
	sourceDBConn, targetDBConn, err := a.getDB()
	if err != nil {
		return fmt.Errorf("cannot connect to DB: %w", err)
	}
	defer sourceDBConn.Close()
	defer targetDBConn.Close()

	where := ""
	if lastID > 0 {
		where = fmt.Sprintf("WHERE id <= %d", lastID)
		a.Logger.Infof("checking first changeset to changeset id: %d", lastID)
	}

	// Compare changeset records one by one

	// TODO: Confirm total changeset count
	// TODO: Confirm changeset start and end IDs match

	sql := fmt.Sprintf(`
			SELECT
				-- excludes the timestamp field
				action, schema_name, table_name, new_values, old_values
			FROM warp_pipe.changesets %s
			ORDER BY id`, where)

	sRows, err := sourceDBConn.Query(sql)
	if err != nil {
		return fmt.Errorf("failed to get source changesets table rows: %w", err)
	}

	tRows, err := targetDBConn.Query(sql)
	if err != nil {
		return fmt.Errorf("failed to get target changesets table rows: %w", err)
	}

	countLog := 0
	countDiff := 0
	for sRows.Next() {
		if !tRows.Next() {
			return fmt.Errorf("target missing expected changeset records")
		}

		sEvent, err := scanRow(sRows)
		if err != nil {
			return fmt.Errorf("failed to load source changeset row: %w", err)
		}

		tEvent, err := scanRow(tRows)
		if err != nil {
			return fmt.Errorf("failed to load target changeset row: %w", err)
		}

		countLog++
		if countLog == 1000 {
			// Log a message every 1000 changeset records
			a.Logger.Infof("1000 changesets processed", tEvent.ID)
			countLog = 0
		}

		if !reflect.DeepEqual(sEvent, tEvent) {
			a.Logger.Errorf("source/target rows differ, source: %+v target: %+v", sEvent, tEvent)
			countDiff++
		}
		if countDiff == 100 {
			a.Logger.Errorf("100 different records found, stopping check")
			break
		}
	}

	// TODO: Compare tables directly, requires DB maintenance mode enabled to avoid new data. Is needed?
	diffFound := countDiff > 0
	if diffFound {
		a.Logger.Info("verbose check failed")
		return fmt.Errorf("changeset records checksums differ")
	}
	a.Logger.Info("verbose check passed")
	return nil
}

func scanRow(rows *pgx.Rows) (*store.Event, error) {
	var evt store.Event
	err := rows.Scan(
		//&evt.ID, Not Sequential :(
		//&evt.Timestamp ignored
		&evt.Action,
		&evt.SchemaName,
		&evt.TableName,
		// &evt.OID ignored
		&evt.NewValues,
		&evt.OldValues,
	)

	return &evt, err
}

// Shutdown the Axon worker.
func (a *Axon) Shutdown() {
	a.shutdownCh <- syscall.SIGTERM
}

func (a *Axon) processChange(sourceDB *sqlx.DB, targetDB *sqlx.DB, change *Changeset) error {
	var err error

	switch change.Kind {
	case ChangesetKindInsert:
		err = a.processInsert(sourceDB, targetDB, change)
	case ChangesetKindUpdate:
		err = a.processUpdate(targetDB, change)
	case ChangesetKindDelete:
		err = a.processDelete(targetDB, change)
	}

	return err
}

func (a *Axon) processDelete(targetDB *sqlx.DB, change *Changeset) error {
	pk, err := getPrimaryKeyForChange(change)
	if err != nil {
		a.Logger.WithError(err).WithField("table", change.Table).
			Errorf("unable to process DELETE for table '%s', changeset has no primary key", change.Table)
		return err
	}

	err = deleteRow(targetDB, change, pk)
	if err != nil {
		a.Logger.WithError(err).WithField("table", change.Table).
			Errorf("failed to DELETE row for table '%s' (pk: %s)", change.Table, pk)
		return err
	}

	return nil
}

func (a *Axon) processInsert(sourceDB *sqlx.DB, targetDB *sqlx.DB, change *Changeset) error {
	err := insertRow(sourceDB, targetDB, change, a.Config.FailOnDuplicate)
	if err != nil {
		a.Logger.WithError(err).WithField("table", change.Table).
			Errorf("failed to INSERT row for table '%s'", change.Table)
		return err
	}

	return nil
}

func (a *Axon) processUpdate(targetDB *sqlx.DB, change *Changeset) error {
	pk, err := getPrimaryKeyForChange(change)
	if err != nil {
		a.Logger.WithError(err).WithField("table", change.Table).
			Errorf("unable to process UPDATE for table '%s', changeset has no primary key", change.Table)
		return err
	}

	err = updateRow(targetDB, change, pk)
	if err != nil {
		a.Logger.WithError(err).WithField("table", change.Table).
			Errorf("failed to UPDATE row for table '%s' (pk: %s)", change.Table, pk)
		return err
	}

	return nil
}
