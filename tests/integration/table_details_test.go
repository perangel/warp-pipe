package integration

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/jackc/pgx"
	"github.com/perangel/warp-pipe/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerateTablesList(t *testing.T) {
	// Re-used test case elements
	defaultTableCreateQueries := []string{
		`CREATE TABLE "test1" (id1 INT PRIMARY KEY, content TEXT);`,
		`CREATE TABLE "test2" (id2 UUID PRIMARY KEY, content BYTEA);`,
		`CREATE TABLE "test3" (id3 BIGINT PRIMARY KEY, content TIMESTAMP WITH TIME ZONE);`,
	}
	crdbDefaultExpectedTables := []db.Table{
		{Name: "test1", Schema: "public", PKeyName: "primary", PKeyFields: map[int]string{1: "id1"}},
		{Name: "test2", Schema: "public", PKeyName: "primary", PKeyFields: map[int]string{1: "id2"}},
		{Name: "test3", Schema: "public", PKeyName: "primary", PKeyFields: map[int]string{1: "id3"}},
	}
	psqlDefaultExpectedTables := []db.Table{
		{Name: "test1", Schema: "public", PKeyName: "test1_pkey", PKeyFields: map[int]string{1: "id1"}},
		{Name: "test2", Schema: "public", PKeyName: "test2_pkey", PKeyFields: map[int]string{1: "id2"}},
		{Name: "test3", Schema: "public", PKeyName: "test3_pkey", PKeyFields: map[int]string{1: "id3"}},
	}
	psqlEnv := []string{
		fmt.Sprintf("POSTGRES_DB=%s", dbName),
		fmt.Sprintf("POSTGRES_USER=%s", dbUser),
		fmt.Sprintf("POSTGRES_PASSWORD=%s", dbPassword),
	}
	psqlConfig := pgx.ConnConfig{
		User:     dbUser,
		Password: dbPassword,
		Database: dbName,
	}

	for _, tc := range []struct {
		image string
		cmd   []string
		env   []string
		port  int

		config pgx.ConnConfig

		tableCreateQueries []string

		expected []db.Table
	}{
		{
			image:              "postgres:9",
			cmd:                []string{"postgres"},
			env:                psqlEnv,
			port:               5432,
			config:             psqlConfig,
			tableCreateQueries: defaultTableCreateQueries,
			expected:           psqlDefaultExpectedTables,
		},
		{
			image:              "postgres:10",
			cmd:                []string{"postgres"},
			env:                psqlEnv,
			port:               5432,
			config:             psqlConfig,
			tableCreateQueries: defaultTableCreateQueries,
			expected:           psqlDefaultExpectedTables,
		},
		{
			image:              "postgres:11",
			cmd:                []string{"postgres"},
			env:                psqlEnv,
			port:               5432,
			config:             psqlConfig,
			tableCreateQueries: defaultTableCreateQueries,
			expected:           psqlDefaultExpectedTables,
		},
		{
			image:              "postgres:12.6",
			cmd:                []string{"postgres"},
			env:                psqlEnv,
			port:               5432,
			config:             psqlConfig,
			tableCreateQueries: defaultTableCreateQueries,
			expected:           psqlDefaultExpectedTables,
		},
		{
			image:              "cockroachdb/cockroach:latest",
			cmd:                []string{"start-single-node", "--insecure"},
			port:               26257,
			config:             pgx.ConnConfig{User: "root"},
			tableCreateQueries: defaultTableCreateQueries,
			expected:           crdbDefaultExpectedTables,
		},
	} {
		tc := tc
		t.Run(tc.image, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()

			// Create db and connect
			_, port, err := createContainer(t, ctx, tc.image, tc.port, tc.env, tc.cmd)
			assert.NoError(t, err)
			tc.config.Host = "127.0.0.1"
			tc.config.Port = uint16(port)
			assert.True(t, waitForPostgresReady(&tc.config))
			conn, err := pgx.Connect(tc.config)
			require.NoError(t, err)
			defer conn.Close()

			// Create tables
			for _, query := range tc.tableCreateQueries {
				_, err = conn.Exec(query)
				assert.NoError(t, err)
			}

			// Grab tables and assert
			schemas := []string{"public"}
			includeTables := []string{}
			excludeTables := []string{}

			tables, err := db.GenerateTablesList(conn, schemas, includeTables, excludeTables)
			assert.NoError(t, err)
			sort.Slice(tables, func(i, j int) bool {
				return tables[i].Name < tables[j].Name
			})
			assert.EqualValues(t, tc.expected, tables)
		})
	}
}
