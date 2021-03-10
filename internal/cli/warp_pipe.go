package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx"
	warppipe "github.com/perangel/warp-pipe"
	log "github.com/sirupsen/logrus"

	"github.com/spf13/cobra"
)

// Flags
var (
	dbHost             string
	dbPort             int
	dbName             string
	dbUser             string
	dbPass             string
	replicationMode    string
	ignoreTables       []string
	whitelistTables    []string
	startFromOffset    int64
	startFromTimestamp int64
	startFromLSN       int64
	logLevel           string
)

const (
	replicationModeLR    = "lr"
	replicationModeAudit = "audit"
)

func init() {
	WarpPipeCmd.PersistentFlags().StringVarP(&logLevel, "log-level", "L", "info", "log level")
	WarpPipeCmd.PersistentFlags().StringVarP(&dbHost, "db-host", "H", "", "database host")
	WarpPipeCmd.PersistentFlags().IntVarP(&dbPort, "db-port", "p", 0, "database port")
	WarpPipeCmd.PersistentFlags().StringVarP(&dbName, "db-name", "d", "", "database name")
	WarpPipeCmd.PersistentFlags().StringVarP(&dbUser, "db-user", "U", "", "database user")
	WarpPipeCmd.PersistentFlags().StringVarP(&dbPass, "db-pass", "P", "", "database password")
	WarpPipeCmd.Flags().Int64Var(&startFromLSN, "start-from-lsn", -1, "stream all changes starting from the provided LSN")
	WarpPipeCmd.Flags().Int64Var(&startFromOffset, "start-from-offset", -1, "stream all changes starting from the provided changeset offset")
	WarpPipeCmd.Flags().Int64Var(&startFromTimestamp, "start-from-ts", -1, "stream all changes starting from the provided timestamp")
	WarpPipeCmd.Flags().StringVarP(&replicationMode, "replication-mode", "M", replicationModeLR, "replication mode")
	WarpPipeCmd.Flags().StringSliceVarP(&ignoreTables, "ignore-tables", "i", nil, "tables to ignore during replication")
	WarpPipeCmd.Flags().StringSliceVarP(&whitelistTables, "whitelist-tables", "w", nil, "tables to include during replication")
	WarpPipeCmd.Flags().SortFlags = false

	WarpPipeCmd.AddCommand(
		setupDBCmd,
		teardownDBCmd,
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

		listener, err := initListener(config)
		if err != nil {
			return err
		}

		connConfig := &pgx.ConnConfig{
			Host:     config.Database.Host,
			Port:     uint16(config.Database.Port),
			User:     config.Database.User,
			Password: config.Database.Password,
			Database: config.Database.Database,
		}

		wp, err := warppipe.NewWarpPipe(
			connConfig,
			listener,
			warppipe.IgnoreTables(config.IgnoreTables),
			warppipe.WhitelistTables(config.WhitelistTables),
			warppipe.LogLevel(config.LogLevel),
		)
		if err != nil {
			log.Fatal(err)
		}

		if err := wp.Open(); err != nil {
			log.Fatal(err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		changes, errors := wp.ListenForChanges(ctx)
		go func() {
			for {
				select {
				case change := <-changes:
					b, err := json.Marshal(change)
					if err != nil {
						log.Error(err)
					}
					fmt.Println(string(b))
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
