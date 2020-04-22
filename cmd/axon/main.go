package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/jmoiron/sqlx"
	"github.com/kelseyhightower/envconfig"
	_ "github.com/lib/pq"
	warppipe "github.com/perangel/warp-pipe"
	"github.com/sirupsen/logrus"
)

var (
	logger *logrus.Logger
)

func init() {
	logger = logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})
}

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

func parseConfig() *Config {
	var config Config
	err := envconfig.Process("axon", &config)
	if err != nil {
		logger.WithError(err).Fatal("failed to process environment config")
	}
	return &config
}

func main() {
	config := parseConfig()
	shutdownCh := make(chan os.Signal)
	signal.Notify(shutdownCh, os.Interrupt, syscall.SIGTERM)

	// TODO: Refactor to use just one connection to the sourceDB
	sourceDBConn, err := sqlx.Open("postgres", getDBConnString(
		config.SourceDBHost,
		config.SourceDBPort,
		config.SourceDBName,
		config.SourceDBUser,
		config.SourceDBPass,
	))
	if err != nil {
		logger.WithError(err).Fatal("unable to connect to source database")
	}

	targetDBConn, err := sqlx.Open("postgres", getDBConnString(
		config.TargetDBHost,
		config.TargetDBPort,
		config.TargetDBName,
		config.TargetDBUser,
		config.TargetDBPass,
	))
	if err != nil {
		logger.WithError(err).Fatal("unable to connect to target database")
	}

	err = checkTargetVersion(targetDBConn)
	if err != nil {
		logger.WithError(err).Fatal("unable to use target database")
	}

	// TODO: (1) add support for selecting the warp-pipe mode
	// TODO: (2) only print the source stats if that is audit
	err = printSourceStats(sourceDBConn)
	if err != nil {
		logger.WithError(err).Fatal("unable get source db stats")
	}

	err = loadPrimaryKeys(targetDBConn)
	if err != nil {
		logger.WithError(err).Fatal("unable to load target DB primary keys")
	}

	err = loadColumnSequences(targetDBConn)
	if err != nil {
		logger.WithError(err).Fatal("unable to load target DB column sequences")
	}

	err = loadOrphanSequences(sourceDBConn)
	if err != nil {
		logger.WithError(err).Fatal("unable to load source DB orphan sequences")
	}

	// create a notify listener and start from changeset id 1
	listener := warppipe.NewNotifyListener(warppipe.StartFromID(0))
	wp := warppipe.NewWarpPipe(listener)
	err = wp.Open(&warppipe.DBConfig{
		Host:     config.SourceDBHost,
		Port:     config.SourceDBPort,
		Database: config.SourceDBName,
		User:     config.SourceDBUser,
		Password: config.SourceDBPass,
	})
	if err != nil {
		logger.WithError(err).
			WithField("component", "warp_pipe").
			Fatal("unable to connect to source database")
	}

	ctx, cancel := context.WithCancel(context.Background())
	changes, errs := wp.ListenForChanges(ctx)

	go func() {
		<-shutdownCh
		logger.Error("shutting down...")
		cancel()
		wp.Close()
		sourceDBConn.Close()
		targetDBConn.Close()
		os.Exit(0)
	}()

	for {
		select {
		case err := <-errs:
			logger.WithError(err).
				WithField("component", "warp_pipe").
				Error("received an error")
		case change := <-changes:
			processChange(sourceDBConn, targetDBConn, config.TargetDBSchema, change)

		}
	}
}

func processChange(sourceDB *sqlx.DB, targetDB *sqlx.DB, schema string, change *warppipe.Changeset) {
	switch change.Kind {
	case warppipe.ChangesetKindInsert:
		processInsert(sourceDB, targetDB, schema, change)
	case warppipe.ChangesetKindUpdate:
		processUpdate(targetDB, schema, change)
	case warppipe.ChangesetKindDelete:
		processDelete(targetDB, schema, change)
	}
}

func processDelete(targetDB *sqlx.DB, schema string, change *warppipe.Changeset) {
	pk, err := getPrimaryKeyForChange(change)
	if err != nil {
		logger.WithError(err).WithField("table", change.Table).
			Errorf("unable to process DELETE for table '%s', changeset has no primary key", change.Table)
	}

	err = deleteRow(targetDB, schema, change, pk)
	if err != nil {
		logger.WithError(err).WithField("table", change.Table).
			Errorf("failed to DELETE row for table '%s' (pk: %s)", change.Table, pk)
	}
}

func processInsert(sourceDB *sqlx.DB, targetDB *sqlx.DB, schema string, change *warppipe.Changeset) {
	err := insertRow(sourceDB, targetDB, schema, change)
	if err != nil {
		logger.WithError(err).WithField("table", change.Table).
			Errorf("failed to INSERT row for table '%s'", change.Table)
	}
}

func processUpdate(targetDB *sqlx.DB, schema string, change *warppipe.Changeset) {
	pk, err := getPrimaryKeyForChange(change)
	if err != nil {
		logger.WithError(err).WithField("table", change.Table).
			Errorf("unable to process UPDATE for table '%s', changeset has no primary key", change.Table)
	}

	err = updateRow(targetDB, schema, change, pk)
	if err != nil {
		logger.WithError(err).WithField("table", change.Table).
			Errorf("failed to UPDATE row for table '%s' (pk: %s)", change.Table, pk)
	}
}
