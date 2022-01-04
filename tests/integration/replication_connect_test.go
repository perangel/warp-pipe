package integration

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type pgsLogWrap struct {
	logger *log.Entry
}

func (l *pgsLogWrap) Log(level pgx.LogLevel, msg string, data map[string]interface{}) {
	l.logger.WithFields(data).Log(log.Level(level), msg)
}

func TestReplicationConnect(t *testing.T) {
	buildSha := os.Getenv("BUILD_SHA")
	assert.NotEmpty(t, buildSha)
	ctx := context.Background()

	for _, tc := range []struct {
		image                         string
		expectReplicationConnectError bool
	}{
		{
			image:                         "postgres:9",
			expectReplicationConnectError: true,
		},
		{
			image: "postgres:10",
		},
		{
			image: "postgres:11",
		},
		{
			image: "postgres:12",
		},
		{
			image:                         "psql-int-test:9-" + buildSha,
			expectReplicationConnectError: true,
		},
		{
			image: "psql-int-test:10-" + buildSha,
		},
		{
			image: "psql-int-test:11-" + buildSha,
		},
		{
			image: "psql-int-test:12-" + buildSha,
		},
	} {
		tc := tc // capture range variable <https://gist.github.com/posener/92a55c4cd441fc5e5e85f27bca008721#how-to-solve-this>
		t.Run(tc.image, func(t *testing.T) {
			t.Parallel()

			_, srcPort, err := createDatabaseContainer(t, ctx, tc.image, dbUser, dbPassword, dbName)
			require.NoError(t, err)
			time.Sleep(10 * time.Second) // Wait for postgres to start

			srcDBConfig := pgx.ConnConfig{
				Host:     "127.0.0.1",
				Port:     uint16(srcPort),
				User:     dbUser,
				Password: dbPassword,
				Database: dbName,
				Logger:   &pgsLogWrap{log.NewEntry(log.New())},
			}

			_, err = pgx.Connect(srcDBConfig)
			require.NoError(t, err)

			_, err = pgx.ReplicationConnect(srcDBConfig)
			if tc.expectReplicationConnectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

		})
	}
}
