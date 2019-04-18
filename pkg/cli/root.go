package cli

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/perangel/warp-pipe/pkg/warp-pipe"
	log "github.com/sirupsen/logrus"

	"github.com/spf13/cobra"
)

// Flags
var (
	dbHost string
	dbPort int16
	dbName string
	dbUser string
	dbPass string
	wpMode string
)

const (
	wpModeLogicalReplication = "lr"
	wpModeQueue              = "queue"
)

// WarpPipeCmd is the root command.
var WarpPipeCmd = &cobra.Command{
	Use:   "warp-pipe",
	Short: "Run a warp-pipe",
	Long:  `Run a warp-pipe and stream changes from a Postgres database.`,
	Run: func(cmd *cobra.Command, _ []string) {
		config := parseConfig()
		wp := warppipe.NewWarpPipe(config)
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
				case <-time.Tick(100 * time.Millisecond):
					continue
				}
			}
		}()

		shutdownCh := make(chan os.Signal, 1)
		signal.Notify(shutdownCh, os.Interrupt, syscall.SIGTERM)
		for {
			<-shutdownCh
			cancel()
			if err := wp.Close(); err != nil {
				log.Error(err)
			}
			return
		}
	},
}

func parseConfig() *warppipe.Config {
	wpConfig := warppipe.NewConfigFromEnv()

	if dbHost != "" {
		wpConfig.DBHost = dbHost
	}

	if dbPort != 0 {
		wpConfig.DBPort = uint16(dbPort)
	}

	if dbUser != "" {
		wpConfig.DBUser = dbUser
	}

	if dbPass != "" {
		wpConfig.DBPass = dbPass
	}

	if dbName != "" {
		wpConfig.DBName = dbName
	}

	if wpMode != "" {
		switch wpMode {
		case wpModeLogicalReplication:
			wpConfig.ListenerType = warppipe.ListenerTypeLogicalReplication
		case wpModeQueue:
			wpConfig.ListenerType = warppipe.ListenerTypeNotify
		default:
			log.Fatal("invalid value for `--mode`, must be one of (`lr`, `queue`)")
		}
	}

	return wpConfig
}

func init() {
	WarpPipeCmd.Flags().StringVarP(&wpMode, "mode", "M", "lr", "replication mode")
	WarpPipeCmd.Flags().StringVarP(&dbHost, "db-host", "H", "", "database host")
	WarpPipeCmd.Flags().Int16VarP(&dbPort, "db-port", "p", 0, "database port")
	WarpPipeCmd.Flags().StringVarP(&dbName, "db-name", "d", "", "database name")
	WarpPipeCmd.Flags().StringVarP(&dbUser, "db-user", "U", "", "database user")
	WarpPipeCmd.Flags().StringVarP(&dbPass, "db-pass", "P", "", "database password")
	WarpPipeCmd.Flags().SortFlags = false
}
