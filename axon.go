package warppipe

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/jackc/pgx"
	"github.com/jmoiron/sqlx"
	"github.com/kelseyhightower/envconfig"
	"github.com/perangel/warp-pipe/db"
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
func (a *Axon) Run() {
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
		a.Logger.WithError(err).Fatal("unable to connect to source database")
	}

	targetDBConn, err := sqlx.Open("postgres", getDBConnString(
		a.Config.TargetDBHost,
		a.Config.TargetDBPort,
		a.Config.TargetDBName,
		a.Config.TargetDBUser,
		a.Config.TargetDBPass,
	))
	if err != nil {
		a.Logger.WithError(err).Fatal("unable to connect to target database")
	}

	err = checkTargetVersion(targetDBConn)
	if err != nil {
		a.Logger.WithError(err).Fatal("unable to check target database version")
	}

	// TODO: (1) add support for selecting the warp-pipe mode
	// TODO: (2) only print the source stats if that is audit
	err = printSourceStats(sourceDBConn)
	if err != nil {
		a.Logger.WithError(err).Fatal("unable to get source db stats")
	}

	err = loadPrimaryKeys(targetDBConn)
	if err != nil {
		a.Logger.WithError(err).Fatal("unable to load target DB primary keys")
	}

	err = loadColumnSequences(targetDBConn)
	if err != nil {
		a.Logger.WithError(err).Fatal("unable to load target DB column sequences")
	}

	err = loadOrphanSequences(sourceDBConn)
	if err != nil {
		a.Logger.WithError(err).Fatal("unable to load source DB orphan sequences")
	}

	// create a notify listener and start from changeset id 1
	listener := NewNotifyListener(StartFromID(0))

	connConfig := pgx.ConnConfig{
		Host:     a.Config.SourceDBHost,
		Port:     uint16(a.Config.SourceDBPort),
		User:     a.Config.SourceDBUser,
		Password: a.Config.SourceDBPass,
		Database: a.Config.SourceDBName,
	}

	wp, err := NewWarpPipe(&connConfig, listener)
	if err != nil {
		a.Logger.WithError(err).
			WithField("component", "warp_pipe").
			Fatal("failed to establish a warp-pipe")
	}

	err = wp.Open()
	if err != nil {
		a.Logger.WithError(err).
			WithField("component", "warp_pipe").
			Fatal("failed to dial the listener")
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
			return
		case err := <-errs:
			a.Logger.WithError(err).
				WithField("component", "warp_pipe").
				Error("received an error")
		case change := <-changes:
			a.processChange(sourceDBConn, targetDBConn, a.Config.TargetDBSchema, change)
			if a.Config.ShutdownAfterLastChangeset {
				isLatest, err := wp.IsLatestChangeSet(change.ID)
				if err != nil {
					a.Logger.WithError(err).
						WithField("component", "warp_pipe").
						Fatal("failed to determine if the sync is complete")
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

func (a *Axon) Verify(schemas, includeTables, excludeTables []string) error {

	if a.Logger == nil {
		a.Logger = logrus.New()
		a.Logger.SetFormatter(&logrus.JSONFormatter{})
	}

	sourceDBConn, err := pgx.Connect(pgx.ConnConfig{
		Host:     a.Config.SourceDBHost,
		Port:     uint16(a.Config.SourceDBPort),
		User:     a.Config.SourceDBUser,
		Password: a.Config.SourceDBPass,
		Database: a.Config.SourceDBName,
	})
	if err != nil {
		return fmt.Errorf("unable to connect to source database: %w", err)
	}

	err = db.PrepareForDataIntegrityChecks(sourceDBConn)
	if err != nil {
		return fmt.Errorf("unable to prepare source database for Integrity checks: %w", err)
	}

	targetDBConn, err := pgx.Connect(pgx.ConnConfig{
		Host:     a.Config.TargetDBHost,
		Port:     uint16(a.Config.TargetDBPort),
		User:     a.Config.TargetDBUser,
		Password: a.Config.TargetDBPass,
		Database: a.Config.TargetDBName,
	})
	if err != nil {
		return fmt.Errorf("unable to connect to target database: %w", err)
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
			return fmt.Errorf(`table "%s"."%s" has no primary key, cannot guarantee checksum match.`, table.Schema, table.Name)
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
			return fmt.Errorf("checksums differ for table %s.%s", table.Schema, table.Name)
		}
	}
	return nil
}

// Shutdown the Axon worker.
func (a *Axon) Shutdown() {
	a.shutdownCh <- syscall.SIGTERM
}

func (a *Axon) processChange(sourceDB *sqlx.DB, targetDB *sqlx.DB, schema string, change *Changeset) {
	switch change.Kind {
	case ChangesetKindInsert:
		a.processInsert(sourceDB, targetDB, schema, change)
	case ChangesetKindUpdate:
		a.processUpdate(targetDB, schema, change)
	case ChangesetKindDelete:
		a.processDelete(targetDB, schema, change)
	}
}

func (a *Axon) processDelete(targetDB *sqlx.DB, schema string, change *Changeset) {
	pk, err := getPrimaryKeyForChange(change)
	if err != nil {
		a.Logger.WithError(err).WithField("table", change.Table).
			Errorf("unable to process DELETE for table '%s', changeset has no primary key", change.Table)
	}

	err = deleteRow(targetDB, schema, change, pk)
	if err != nil {
		a.Logger.WithError(err).WithField("table", change.Table).
			Errorf("failed to DELETE row for table '%s' (pk: %s)", change.Table, pk)
	}
}

func (a *Axon) processInsert(sourceDB *sqlx.DB, targetDB *sqlx.DB, schema string, change *Changeset) {
	err := insertRow(sourceDB, targetDB, schema, change)
	if err != nil {
		a.Logger.WithError(err).WithField("table", change.Table).
			Errorf("failed to INSERT row for table '%s'", change.Table)
	}
}

func (a *Axon) processUpdate(targetDB *sqlx.DB, schema string, change *Changeset) {
	pk, err := getPrimaryKeyForChange(change)
	if err != nil {
		a.Logger.WithError(err).WithField("table", change.Table).
			Errorf("unable to process UPDATE for table '%s', changeset has no primary key", change.Table)
	}

	err = updateRow(targetDB, schema, change, pk)
	if err != nil {
		a.Logger.WithError(err).WithField("table", change.Table).
			Errorf("failed to UPDATE row for table '%s' (pk: %s)", change.Table, pk)
	}
}
