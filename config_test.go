package warppipe_test

import (
	"os"
	"testing"

	warppipe "github.com/perangel/warp-pipe"
	"github.com/stretchr/testify/assert"
)

func TestNewConfigFromEnv(t *testing.T) {
	t.Run("test with namespace", func(t *testing.T) {
		os.Setenv("REPLICATION_MODE", "lr")
		os.Setenv("IGNORE_TABLES", "posts,comments")
		os.Setenv("WHITELIST_TABLES", "users,pets")
		os.Setenv("LOG_LEVEL", "info")
	})

	config, err := warppipe.NewConfigFromEnv()
	assert.NoError(t, err)
	assert.Equal(t, "lr", config.ReplicationMode)
	assert.Equal(t, []string{"users", "pets"}, config.WhitelistTables)
	assert.Equal(t, []string{"posts", "comments"}, config.IgnoreTables)
	assert.Equal(t, "info", config.LogLevel)

	t.Run("test with no namespace", func(t *testing.T) {

	})
}
