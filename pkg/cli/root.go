package cli

import (
	log "github.com/sirupsen/logrus"

	"github.com/jackc/pgx"
	"github.com/perangel/warp-pipe/pkg/listener"
	"github.com/spf13/cobra"
)

// Flags
var (
	dbHost string
	dbPort int16
	dbName string
	dbUser string
	dbPass string
)

// WarpPipeCmd is the root command.
var WarpPipeCmd = &cobra.Command{
	Use:   "warp-pipe",
	Short: "Run a warp-pipe",
	Long:  `Run a warp-pipe and stream changes from a Postgres database.`,
	Run: func(cmd *cobra.Command, _ []string) {
		// TODO: configure listener
		pgConnConfig := &pgx.ConnConfig{
			Host:     dbHost,
			Port:     uint16(dbPort),
			User:     dbUser,
			Password: dbPass,
			Database: dbName,
		}

		listener := listener.NewLogicalReplicationListener()
		err := listener.Dial(pgConnConfig)
		if err != nil {
			log.Fatal(err)
		}

		_, _ = listener.ListenForChanges()

		for {
		}
	},
}

func init() {
	WarpPipeCmd.Flags().StringVarP(&dbHost, "db-host", "H", "", "database host")
	WarpPipeCmd.Flags().Int16VarP(&dbPort, "db-port", "p", 0, "database port")
	WarpPipeCmd.Flags().StringVarP(&dbName, "db-name", "d", "", "database name")
	WarpPipeCmd.Flags().StringVarP(&dbUser, "db-user", "U", "", "database user")
	WarpPipeCmd.Flags().StringVarP(&dbPass, "db-pass", "P", "", "database password")
}
