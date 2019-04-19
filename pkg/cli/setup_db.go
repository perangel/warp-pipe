package cli

import (
	"github.com/jackc/pgx"
	"github.com/perangel/warp-pipe/pkg/db"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// Flags
var (
	setupDBIgnoreTables []string
	setupDBSchema       string
)

var setupDBCmd = &cobra.Command{
	Use:   "setup-db",
	Short: "Setup the source database",
	Long: `Setup the source database for tracking changesets.

This command adds a new 'warp_pipe' schema with a 'changesets' table to the source
database, and registers a TRIGGER that will write all table changes after INSERT,
UPDATE, or DELETE to the 'warp_pipe.changesets' table.

Once this is setup, you can run 'warp-pipe' with the 'queue' listener to stream
the changesets.

For more details see: https://github.com/perangel/warp-pipe/docs/setup_database.md
	`,
	Run: func(cmd *cobra.Command, _ []string) {
		dbConfig := &pgx.ConnConfig{
			Host:     dbHost,
			Port:     uint16(dbPort),
			User:     dbUser,
			Password: dbPass,
			Database: dbName,
		}

		conn, err := pgx.Connect(*dbConfig)
		if err != nil {
			log.WithError(err).Fatal("unable to connect to database")
		}

		err = db.SetupDatabase(conn, setupDBSchema, setupDBIgnoreTables)
		if err != nil {
			log.WithError(err).Fatal("failed to setup database for replication")
		}
	},
}

func init() {
	setupDBCmd.Flags().StringSliceVarP(&setupDBIgnoreTables, "ignore-tables", "I", nil, "tables to exclude from replication")
	setupDBCmd.Flags().StringVarP(&setupDBSchema, "schema", "s", "public", "schema to setup for replication")
}
