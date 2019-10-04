package cli

import (
	"fmt"

	"github.com/jackc/pgx"
	"github.com/perangel/warp-pipe/internal/db"
	"github.com/spf13/cobra"
)

// Flags
var (
	setupDBSchemas         []string
	setupDBIgnoreTables    []string
	setupDBWhitelistTables []string
	setupDBReplicaIdentity string
)

var setupDBCmd = &cobra.Command{
	Use:   "setup-db",
	Short: "Setup the source database",
	Long: `Setup the source database for tracking changesets.

This command adds a new 'warp_pipe' schema with a 'changesets' table to the source
database, and registers a trigger that will write all table changes after INSERT,
UPDATE, or DELETE to the 'warp_pipe.changesets' table.

Once this is setup, you can run 'warp-pipe' with the 'audit' listener to stream
the changesets.

For more details see: https://github.com/perangel/warp-pipe/docs/setup_database.md
	`,
	RunE: func(cmd *cobra.Command, _ []string) error {
		config, err := parseConfig()
		if err != nil {
			return err
		}

		dbConfig := &pgx.ConnConfig{
			Host:     config.Database.Host,
			Port:     uint16(config.Database.Port),
			User:     config.Database.User,
			Password: config.Database.Password,
			Database: config.Database.Database,
		}

		conn, err := pgx.Connect(*dbConfig)
		if err != nil {
			return err
		}

		err = db.Prepare(conn, setupDBSchemas, setupDBWhitelistTables, setupDBIgnoreTables)
		if err != nil {
			return err
		}

		fmt.Println("Successfully created `warp_pipe` schema")
		return nil
	},
}

var teardownDBCmd = &cobra.Command{
	Use:   "teardown-db",
	Short: "Teardown the `warp_pipe` schema ",
	Long:  `Teardown the 'warp_pipe' schema in the source database.`,
	RunE: func(cmd *cobra.Command, _ []string) error {
		config, err := parseConfig()
		if err != nil {
			return err
		}

		dbConfig := &pgx.ConnConfig{
			Host:     config.Database.Host,
			Port:     uint16(config.Database.Port),
			User:     config.Database.User,
			Password: config.Database.Password,
			Database: config.Database.Database,
		}

		conn, err := pgx.Connect(*dbConfig)
		if err != nil {
			return err
		}

		err = db.Teardown(conn)
		if err != nil {
			return err
		}

		fmt.Println("Successfully removed `warp_pipe` schema")
		return nil
	},
}

func init() {
	setupDBCmd.Flags().StringSliceVarP(&setupDBIgnoreTables, "ignore-tables", "i", nil, "tables to exclude from replication setup")
	setupDBCmd.Flags().StringSliceVarP(&setupDBWhitelistTables, "whitelist-tables", "w", nil, "tables to include in replication setup")
	setupDBCmd.Flags().StringSliceVarP(&setupDBSchemas, "schemas", "S", []string{"public"}, "schemas to setup for replication")
}
