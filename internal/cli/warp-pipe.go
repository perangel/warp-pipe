package cli

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	warppipe "github.com/perangel/warp-pipe/pkg/warp-pipe"
	log "github.com/sirupsen/logrus"

	"github.com/spf13/cobra"
)

// Flags
var (
	dbHost          string
	dbPort          int16
	dbName          string
	dbUser          string
	dbPass          string
	dbSchema        string
	ignoreTables    []string
	whitelistTables []string
	replicationMode string
	logLevel        string
)

const (
	replicationModeLR    = "lr"
	replicationModeQueue = "queue"
)

func init() {
	WarpPipeCmd.PersistentFlags().StringVarP(&logLevel, "log-level", "L", "info", "log level")
	WarpPipeCmd.PersistentFlags().StringVarP(&dbHost, "db-host", "H", "", "database host")
	WarpPipeCmd.PersistentFlags().Int16VarP(&dbPort, "db-port", "p", 0, "database port")
	WarpPipeCmd.PersistentFlags().StringVarP(&dbName, "db-name", "d", "", "database name")
	WarpPipeCmd.PersistentFlags().StringVarP(&dbUser, "db-user", "U", "", "database user")
	WarpPipeCmd.PersistentFlags().StringVarP(&dbPass, "db-pass", "P", "", "database password")
	WarpPipeCmd.Flags().StringVarP(&dbSchema, "db-schema", "S", "public", "database schema to replicate")
	WarpPipeCmd.Flags().StringVarP(&replicationMode, "replication-mode", "M", replicationModeLR, "replication mode")
	WarpPipeCmd.Flags().StringSliceVarP(&ignoreTables, "ignore-tables", "i", nil, "tables to ignore during replication")
	WarpPipeCmd.Flags().StringSliceVarP(&whitelistTables, "whitelist-tables", "w", nil, "tables to include during replication")
	WarpPipeCmd.Flags().SortFlags = false

	WarpPipeCmd.AddCommand(
		setupDBCmd,
	)
}

// WarpPipeCmd is the root command.
var WarpPipeCmd = &cobra.Command{
	Use:   "warp-pipe",
	Short: "Run a warp-pipe",
	Long:  `Run a warp-pipe and stream changes from a Postgres database.`,
	RunE: func(cmd *cobra.Command, _ []string) error {
		config, err := parseConfig()
		if err != nil {
			return err
		}

		replMode, err := parseReplicationMode(config.ReplicationMode)
		if err != nil {
			return err
		}

		logLvl, err := parseLogLevel(config.LogLevel)
		if err != nil {
			return err
		}

		wp := warppipe.NewWarpPipe(
			&config.DBConfig,
			warppipe.Mode(replMode),
			warppipe.DatabaseSchema(dbSchema),
			warppipe.IgnoreTables(ignoreTables),
			warppipe.WhitelistTables(whitelistTables),
			warppipe.LoggingLevel(logLvl),
		)
		if err := wp.Open(); err != nil {
			log.Fatal(err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		changes, errors := wp.ListenForChanges(ctx)
		go func() {
			for {
				select {
				case change := <-changes:
					log.Printf("%+v\n", change)
				case err := <-errors:
					log.Error(err)
				}
			}
		}()

		shutdownCh := make(chan os.Signal, 1)
		signal.Notify(shutdownCh, os.Interrupt, syscall.SIGTERM)
		for {
			<-shutdownCh
			cancel()
			if err := wp.Close(); err != nil {
				return err
			}
			return nil
		}
	},
}
