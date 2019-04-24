package warppipe

import (
	"github.com/perangel/warp-pipe/internal/listener"
	"github.com/sirupsen/logrus"
)

// ReplMode is the type for valid warp pipe replication modes.
type ReplMode string

// LogLevel is the type for valid warp pipe log levels.
type LogLevel logrus.Level

// WarpPipe option constants
const (
	// modes
	LRMode    ReplMode = "lr"
	QueueMode ReplMode = "queue"

	// log-levels
	LogLevelTrace LogLevel = LogLevel(logrus.TraceLevel)
	LogLevelDebug LogLevel = LogLevel(logrus.DebugLevel)
	LogLevelInfo  LogLevel = LogLevel(logrus.InfoLevel)
	LogLevelWarn  LogLevel = LogLevel(logrus.WarnLevel)
	LogLevelError LogLevel = LogLevel(logrus.ErrorLevel)
	LogLevelFatal LogLevel = LogLevel(logrus.FatalLevel)
)

// Option is a WrapPipe option function
type Option func(*WarpPipe)

// Mode is a option for setting the replication mode.
func Mode(m ReplMode) Option {
	return func(w *WarpPipe) {
		switch m {
		case LRMode:
			w.listener = listener.NewLogicalReplicationListener()
		case QueueMode:
			w.listener = listener.NewNotifyListener()
		}
	}
}

// ReplSlotName is an option for setting the replication slot name.
// This is only valid when running in `LRMode`.
func ReplSlotName(name string) Option {
	return func(w *WarpPipe) {
		w.replSlotName = name
	}
}

// DatabaseSchema is an option for setting the database schema to replicate.
func DatabaseSchema(schema string) Option {
	return func(w *WarpPipe) {
		w.dbSchema = schema
	}
}

// IgnoreTables is an option for setting the tables that WarpPipe should ignore.
// It accepts entries in either of the following formats:
//     - schema.table
//     - schema.*
//     - table
// Any tables in this list will negate any whitelisted tables set via WhitelistTables().
func IgnoreTables(tables []string) Option {
	return func(w *WarpPipe) {
		w.ignoreTables = tables
	}
}

// WhitelistTables is an option for setting a list of tables we want to listen for change from.
// It accepts entries in either of the following formats:
//     - schema.table
//     - schema.*
//     - table
// Any tables set via IgnoreTables() will be excluded.
func WhitelistTables(tables []string) Option {
	return func(w *WarpPipe) {
		w.whitelistTables = tables
	}
}

// LoggingLevel is an option for setting the log level on a WarpPipe.
func LoggingLevel(level LogLevel) Option {
	return func(w *WarpPipe) {
		w.logger.Level = logrus.Level(level)
	}
}
