package integration

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx"
	warppipe "github.com/perangel/warp-pipe"
	"github.com/perangel/warp-pipe/db"
	"github.com/stretchr/testify/require"
)

type TestData struct {
	text    string
	date    time.Time
	boolean bool
	json    string
	jsonb   string
	array   []int32
}

var (
	dbUser     = "test"
	dbPassword = "test"
	dbName     = "test"
	testSchema = []string{
		`CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`,
		`CREATE TABLE test (
			id UUID PRIMARY KEY DEFAULT uuid_generate_v1mc(),
			type_text TEXT,
			type_date DATE,
			type_boolean BOOLEAN,
			type_json JSON,
			type_jsonb JSONB,
			type_array int4[]
		)`,
	}

	insertSQL = `INSERT INTO
	test(type_text, type_date, type_boolean, type_json, type_jsonb, type_array)
	VALUES ($1, $2, $3, $4, $5, $6);`
)

func setupTestSchema(config pgx.ConnConfig) error {
	//wait until db is ready to obtain connection
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
			return fmt.Errorf("Test schema installation failed: %v", err)
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
			defer conn.Close()
			connected = true
			break
		}
		time.Sleep(2 * time.Second)
	}
	return connected
}

func createDatabaseContainer(t *testing.T, ctx context.Context, version string, database string, username string, password string) (string, int, error) {
	docker, err := NewDockerClient()
	if err != nil {
		return "", 0, err
	}
	postgresPort := 5432
	hostPort, err := getFreePort()
	if err != nil {
		return "", 0, errors.New("could not determine a free port")
	}
	container, err := docker.runContainer(
		ctx,
		&ContainerConfig{
			image: fmt.Sprintf("postgres:%s", version),
			ports: []*PortMapping{
				&PortMapping{
					HostPort:      fmt.Sprintf("%d", hostPort),
					ContainerPort: fmt.Sprintf("%d", postgresPort),
				},
			},
			env: []string{
				fmt.Sprintf("POSTGRES_DB=%s", database),
				fmt.Sprintf("POSTGRES_USER=%s", username),
				fmt.Sprintf("POSTGRES_PASSWORD=%s", password),
			},
			cmd: []string{
				"postgres",
				"-cwal_level=logical",
				"-cmax_replication_slots=1",
				"-cmax_wal_senders=1",
			},
		})
	if err != nil {
		return "", 0, err
	}

	t.Cleanup(func() {
		if err := docker.removeContainer(ctx, container.ID); err != nil {
			t.Errorf("Could not remove container %s: %w", container.ID, err)
		}
	})

	return container.ID, hostPort, nil
}

func testRow() *TestData {
	row := TestData{}
	row.text = "a test string"
	row.boolean = false
	row.date = time.Now()
	row.json = `{"name": "Alice", "age": 31, "city": "LA"}`
	row.jsonb = `{"name": "Bob", "age": 39, "city": "London"}`
	row.array = []int32{1, 2, 3, 4, 5}
	return &row
}

func writeTestData(config pgx.ConnConfig, n int, results chan<- int) {
	rowsWritten := 0
	conn, err := pgx.Connect(config)
	if err != nil {
		results <- rowsWritten
		return
	}
	defer conn.Close()
	for i := 0; i < n; i++ {
		row := testRow()
		_, err = conn.Exec(insertSQL, row.text, row.date, row.boolean, row.json, row.jsonb, row.array)
		if err != nil {
			fmt.Printf("oops: %v", err)
			results <- rowsWritten
			return
		}
		rowsWritten = rowsWritten + 1
	}
	results <- rowsWritten
}

func TestVersionMigration(t *testing.T) {
	testCases := []struct {
		name   string
		source string
		target string
	}{
		{
			name:   "9.5To9.6",
			source: "9.5",
			target: "9.6",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()

			//bring up source and target database containers
			_, srcPort, err := createDatabaseContainer(t, ctx, tc.source, dbUser, dbPassword, dbName)
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

			_, targetPort, err := createDatabaseContainer(t, ctx, tc.target, dbUser, dbPassword, dbName)
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
			wpConn, err := pgx.Connect(srcDBConfig)
			require.NoError(t, err)
			err = db.Prepare(wpConn, []string{"public"}, []string{"test"}, []string{})
			if err != nil {
				t.Errorf("Could not setup warp pipe: %v", err)
			}

			// write rows to the source database
			writersCount := 20
			rowsWritten := make(chan int, writersCount)
			for i := 0; i < writersCount; i++ {
				go writeTestData(srcDBConfig, 50, rowsWritten)
			}

			// sync source and target with Axon
			axonCfg := warppipe.AxonConfig{
				SourceDBHost:   srcDBConfig.Host,
				SourceDBPort:   srcPort,
				SourceDBName:   srcDBConfig.Database,
				SourceDBUser:   srcDBConfig.User,
				SourceDBPass:   srcDBConfig.Password,
				TargetDBHost:   targetDBConfig.Host,
				TargetDBPort:   targetPort,
				TargetDBName:   targetDBConfig.Database,
				TargetDBUser:   targetDBConfig.User,
				TargetDBPass:   targetDBConfig.Password,
				TargetDBSchema: "public",
			}

			axon := warppipe.Axon{Config: &axonCfg}
			axon.Run()

			// verify sync
			totalRowsWritten := 0
			for i := 0; i < writersCount; i++ {
				r := <-rowsWritten
				totalRowsWritten = totalRowsWritten + r
			}
			t.Logf("%d rows were written to source database", totalRowsWritten)

			c, err := pgx.Connect(targetDBConfig)
			if err != nil {
				t.Errorf("Could not connect to target database to verify sync: %v", err)
			}
			defer c.Close()

			rows, err := c.Query("select count(*) from test")
			if err != nil {
				t.Errorf("Could not query target database: %v", err)
			}
			defer rows.Close()
			var rowsSynced int
			for rows.Next() {
				err := rows.Scan(&rowsSynced)
				if err != nil {
					t.Errorf("Could not read the number of rows synced: %v", err)
				}
			}
			if totalRowsWritten != rowsSynced {
				t.Errorf("Expected %d rows, have only %d", totalRowsWritten, rowsSynced)
			}
		})
	}
}
