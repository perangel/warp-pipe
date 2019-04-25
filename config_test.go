package warppipe_test

import (
	"os"
	"testing"

	"github.com/sirupsen/logrus"

	warppipe "github.com/perangel/warp-pipe"
	"github.com/stretchr/testify/assert"
)

func TestNewConfigFromEnv(t *testing.T) {
	t.Run("test with namespace", func(t *testing.T) {
		os.Setenv("WP_REPLICATION_MODE", "lr")
		os.Setenv("WP_IGNORE_TABLES", "posts,comments")
		os.Setenv("WP_WHITELIST_TABLES", "users,pets")
		os.Setenv("WP_LOG_LEVEL", "info")

		config, err := warppipe.NewConfigFromEnv()
		assert.NoError(t, err)
		assert.Equal(t, "lr", config.ReplicationMode)
		assert.Equal(t, []string{"users", "pets"}, config.WhitelistTables)
		assert.Equal(t, []string{"posts", "comments"}, config.IgnoreTables)
		assert.Equal(t, "info", config.LogLevel)
	})

	t.Run("test with no namespace", func(t *testing.T) {
		os.Setenv("REPLICATION_MODE", "lr")
		os.Setenv("IGNORE_TABLES", "posts,comments")
		os.Setenv("WHITELIST_TABLES", "users,pets")
		os.Setenv("LOG_LEVEL", "info")

		config, err := warppipe.NewConfigFromEnv()
		assert.NoError(t, err)
		assert.Equal(t, "lr", config.ReplicationMode)
		assert.Equal(t, []string{"users", "pets"}, config.WhitelistTables)
		assert.Equal(t, []string{"posts", "comments"}, config.IgnoreTables)
		assert.Equal(t, "info", config.LogLevel)
	})

	t.Run("test parse database config", func(t *testing.T) {
		os.Setenv("DB_HOST", "localhost")
		os.Setenv("DB_PORT", "6432")
		os.Setenv("DB_NAME", "test_db")
		os.Setenv("DB_USER", "tester")
		os.Setenv("DB_PASS", "secret")

		config, err := warppipe.NewConfigFromEnv()
		assert.NoError(t, err)
		assert.Equal(t, "localhost", config.Database.Host)
		assert.Equal(t, 6432, config.Database.Port)
		assert.Equal(t, "test_db", config.Database.Database)
		assert.Equal(t, "tester", config.Database.User)
		assert.Equal(t, "secret", config.Database.Password)
	})
}

func TestParseLogLevel(t *testing.T) {
	testCases := []struct {
		level       string
		logrusLevel logrus.Level
		err         bool
	}{
		{
			level:       "debug",
			logrusLevel: logrus.DebugLevel,
			err:         false,
		},
		{
			level:       "info",
			logrusLevel: logrus.InfoLevel,
			err:         false,
		},
		{
			level:       "warn",
			logrusLevel: logrus.WarnLevel,
			err:         false,
		},
		{
			level:       "error",
			logrusLevel: logrus.ErrorLevel,
			err:         false,
		},
		{
			level:       "fatal",
			logrusLevel: logrus.FatalLevel,
			err:         false,
		},
		{
			level:       "invalid",
			logrusLevel: 0,
			err:         true,
		},
	}

	for _, tc := range testCases {
		lvl, err := warppipe.ParseLogLevel(tc.level)
		assert.Equal(t, tc.logrusLevel, lvl)
		if tc.err {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}
	}
}
