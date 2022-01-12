package integration

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	warppipe "github.com/perangel/warp-pipe"
	"github.com/perangel/warp-pipe/db"
)

type testData struct {
	text    string
	date    time.Time
	boolean bool
	json    []byte
	jsonb   []byte
	array   []int32
	bytea   []byte
}

var (
	dbUser     = "test"
	dbPassword = "test"
	dbName     = "test"
	testSchema = []string{
		`CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`,
		`CREATE TABLE "testTable" (
			id UUID PRIMARY KEY DEFAULT uuid_generate_v1mc(),
			type_text TEXT,
			type_date DATE,
			type_boolean BOOLEAN,
			type_json JSON,
			type_jsonb JSONB,
			type_array int4[],
			type_bytea bytea
		)`,
		`CREATE TABLE empty_table (
			id SERIAL PRIMARY KEY
		)`,
	}
)

func setupTestSchema(config pgx.ConnConfig) error {
	// wait until db is ready to obtain connection
	srcReady := waitForPostgresReady(&config)
	if !srcReady {
		return fmt.Errorf("database did not become ready in allowed time")
	}

	conn, err := pgx.Connect(config)
	if err != nil {
		return err
	}
	defer conn.Close()

	for _, s := range testSchema {
		_, err = conn.Exec(s)
		if err != nil {
			return fmt.Errorf("test schema installation failed: %v", err)
		}
	}
	return nil
}

func getFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer l.Close()

	return l.Addr().(*net.TCPAddr).Port, nil
}

func waitForPostgresReady(config *pgx.ConnConfig) bool {
	connected := false
	for count := 0; count < 30; count++ {
		conn, err := pgx.Connect(*config)
		if err == nil {
			connected = true
			conn.Close()
			break
		}
		time.Sleep(2 * time.Second)
	}
	return connected
}

func createPSQLDatabaseContainer(t *testing.T, ctx context.Context, image string, database string, username string, password string) (string, int, error) {
	postgresPort := 5432
	env := []string{
		fmt.Sprintf("POSTGRES_DB=%s", database),
		fmt.Sprintf("POSTGRES_USER=%s", username),
		fmt.Sprintf("POSTGRES_PASSWORD=%s", password),
	}
	cmd := []string{
		"postgres",
		"-cwal_level=logical",
		"-cmax_replication_slots=1",
		"-cmax_wal_senders=1",
	}
	return createContainer(t, ctx, image, postgresPort, env, cmd)
}

func createContainer(t *testing.T, ctx context.Context, image string, port int, env, cmd []string) (string, int, error) {
	docker, err := NewDockerClient()
	if err != nil {
		return "", 0, err
	}

	hostPort, err := getFreePort()
	if err != nil {
		return "", 0, errors.New("could not determine a free port")
	}

	container, err := docker.runContainer(
		ctx,
		&ContainerConfig{
			image: image,
			ports: []*PortMapping{
				{
					HostPort:      fmt.Sprintf("%d", hostPort),
					ContainerPort: fmt.Sprintf("%d", port),
				},
			},
			env: env,
			cmd: cmd,
		})
	if err != nil {
		return "", 0, err
	}

	t.Cleanup(func() {
		if err := docker.removeContainer(ctx, container.ID); err != nil {
			t.Errorf("Could not remove container %s: %v", container.ID, err)
		}
	})

	return container.ID, hostPort, nil
}

func testRows() []testData {
	return []testData{
		// Nil and empty fields.
		{},
		// Empty JSON and slice.
		{
			text:    "text",
			boolean: true,
			date:    time.Now(),
			json:    []byte(`{}`),
			jsonb:   []byte(`{}`),
			array:   make([]int32, 0),
			bytea:   []byte(``),
		},
		// Fully populated fields.
		{
			text:    "a test string",
			boolean: true,
			date:    time.Now(),
			json:    []byte(`{"name": "Alice", "age": 31, "city": "LA"}`),
			jsonb:   []byte(`{"name": "Bob", "age": 39, "city": "London"}`),
			array:   []int32{1, 2, 3, 4, 5},
			bytea:   []byte(`abcdef`),
		},
		// JSON string
		{
			json:  []byte(`"{\"name\": \"Alice\", \"age\": 31, \"city\": \"LA\"}"`),
			jsonb: []byte(`"{\"name\": \"Bob\", \"age\": 39, \"city\": \"London\"}"`),
		},
	}
}

func insertTestData(t *testing.T, config pgx.ConnConfig, nRowsX3 int, wg *sync.WaitGroup) {
	defer wg.Done()

	conn, err := pgx.Connect(config)
	if err != nil {
		t.Errorf("%s: could not connected to source database to insert: %v", t.Name(), err)
		return
	}
	defer conn.Close()

	insertSQL := `
		INSERT INTO
			"testTable" (type_text, type_date, type_boolean, type_json, type_jsonb, type_array, type_bytea)
		VALUES ($1, $2, $3, $4, $5, $6, $7);`

	for i := 0; i < nRowsX3; i++ {
		rows := testRows()
		for _, row := range rows {
			_, err = conn.Exec(insertSQL, row.text, row.date, row.boolean, row.json, row.jsonb, row.array, row.bytea)
			if err != nil {
				t.Errorf("%s: Could not insert row in source database: %v", t.Name(), err)
			}
		}
	}
}

func updateTestData(t *testing.T, config pgx.ConnConfig, nRows int, wg *sync.WaitGroup) {
	defer wg.Done()

	conn, err := pgx.Connect(config)
	if err != nil {
		t.Errorf("%s: could not connected to source database to update: %v", t.Name(), err)
		return
	}
	defer conn.Close()

	// update one field in a random row
	updateSQL := `
		UPDATE "testTable" set type_boolean = NOT type_boolean
		WHERE ID IN (SELECT ID FROM "testTable" ORDER BY RANDOM() LIMIT 1);`

	for i := 0; i < nRows; i++ {
		_, err = conn.Exec(updateSQL)
		if err != nil {
			t.Errorf("%s: Could not update row in source database: %v", t.Name(), err)
		}
	}
}

func deleteTestData(t *testing.T, config pgx.ConnConfig, nRows int, wg *sync.WaitGroup) {
	defer wg.Done()

	conn, err := pgx.Connect(config)
	if err != nil {
		t.Errorf("%s: Could not connect to source db to delete data", t.Name())
		return
	}
	defer conn.Close()

	// delete a random row
	deleteSQL := `DELETE FROM "testTable" WHERE ID IN (SELECT ID FROM "testTable" ORDER BY RANDOM() LIMIT 1);`

	for i := 0; i < nRows; i++ {
		_, err = conn.Exec(deleteSQL)
		if err != nil {
			t.Errorf("%s: Could not delete row in source database: %v", t.Name(), err)
		}
	}
}

func TestVersionMigration(t *testing.T) {
	buildSha := os.Getenv("BUILD_SHA")
	assert.NotEmpty(t, buildSha)

	testCases := []struct {
		name         string
		source       string
		target       string
		listenerMode string
	}{
		{
			name:         "9.5To9.6",
			source:       "postgres:9.5",
			target:       "postgres:9.6",
			listenerMode: warppipe.ListenerModeNotify,
		},
		{
			name:         "custom11To11",
			source:       "psql-int-test:11-" + buildSha,
			target:       "postgres:11-alpine",
			listenerMode: warppipe.ListenerModeNotify,
		},
	}

	for _, tc := range testCases {
		tc := tc // capture range element, required for parallel
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()

			// bring up source and target database containers
			_, srcPort, err := createPSQLDatabaseContainer(t, ctx, tc.source, dbUser, dbPassword, dbName)
			require.NoError(t, err)
			srcDBConfig := pgx.ConnConfig{
				Host:     "127.0.0.1",
				Port:     uint16(srcPort),
				User:     dbUser,
				Password: dbPassword,
				Database: dbName,
			}
			err = setupTestSchema(srcDBConfig)
			require.NoError(t, err)

			_, targetPort, err := createPSQLDatabaseContainer(t, ctx, tc.target, dbUser, dbPassword, dbName)
			require.NoError(t, err)
			targetDBConfig := pgx.ConnConfig{
				Host:     "127.0.0.1",
				Port:     uint16(targetPort),
				User:     dbUser,
				Password: dbPassword,
				Database: dbName,
			}

			err = setupTestSchema(targetDBConfig)
			require.NoError(t, err)

			// setup warp-pipe on source database
			schemas := []string{"public"}
			includeTables := make([]string, 0)
			excludeTables := make([]string, 0)

			// Create connections to source, target
			wpConn, err := pgx.Connect(srcDBConfig)
			require.NoError(t, err)

			targetConn, err := pgx.Connect(targetDBConfig)
			require.NoError(t, err)

			// Setup mode-specifics on source, target
			switch tc.listenerMode {
			case warppipe.ListenerModeNotify:
				err = db.Prepare(wpConn, []string{"public"}, []string{"testTable"}, []string{})
				require.NoError(t, err)

				err = db.Prepare(targetConn, []string{"public"}, []string{"testTable"}, []string{})
				require.NoError(t, err)
			}

			// write, update, delete to produce change sets
			var insertsWG, updatesWG, deletesWG sync.WaitGroup
			workersCount := 10
			for i := 0; i < workersCount; i++ {
				insertsWG.Add(1)
				go insertTestData(t, srcDBConfig, 10, &insertsWG)
			}

			for i := 0; i < workersCount; i++ {
				updatesWG.Add(1)
				go updateTestData(t, srcDBConfig, 10, &updatesWG)
			}

			for i := 0; i < workersCount; i++ {
				deletesWG.Add(1)
				go deleteTestData(t, srcDBConfig, 10, &deletesWG)
			}

			// sync source and target with Axon
			axonCfg := warppipe.AxonConfig{
				SourceDBHost:               srcDBConfig.Host,
				SourceDBPort:               srcPort,
				SourceDBName:               srcDBConfig.Database,
				SourceDBUser:               srcDBConfig.User,
				SourceDBPass:               srcDBConfig.Password,
				TargetDBHost:               targetDBConfig.Host,
				TargetDBPort:               targetPort,
				TargetDBName:               targetDBConfig.Database,
				TargetDBUser:               targetDBConfig.User,
				TargetDBPass:               targetDBConfig.Password,
				TargetDBSchema:             "public",
				ShutdownAfterLastChangeset: true,
				FailOnDuplicate:            true,
				ListenerMode:               tc.listenerMode,
			}

			axon := warppipe.Axon{Config: &axonCfg}

			time.Sleep(100 * time.Millisecond) // allow the writes to be occur.

			t.Log("first pass sync")
			err = axon.Run()
			require.NoError(t, err)

			// wait for all our routines to complete
			insertsWG.Wait()
			updatesWG.Wait()
			deletesWG.Wait()

			for i := 0; i < workersCount; i++ {
				insertsWG.Add(1)
				go insertTestData(t, srcDBConfig, 10, &insertsWG)
			}
			insertsWG.Wait()

			switch tc.listenerMode {
			case warppipe.ListenerModeNotify:
				t.Log("second pass sync. starting from count of Changesets in target, catching any stragglers")
				row, err := targetConn.Query("SELECT count(id) from warp_pipe.changesets")
				assert.NoError(t, err)
				require.True(t, row.Next())
				var count int64
				err = row.Scan(&count)
				assert.NoError(t, err)
				t.Logf("count of changesets in target: %d", count)
				axon.Config.StartFromOffset = count
			case warppipe.ListenerModeLogicalReplication:
				// TODO: implement this
			}

			err = axon.Run()
			require.NoError(t, err)

			switch tc.listenerMode {
			case warppipe.ListenerModeNotify:
				require.NoError(t, axon.VerifyChangesets(-1))
			}

			require.NoError(t, axon.Verify(schemas, includeTables, excludeTables))
		})
	}
}
