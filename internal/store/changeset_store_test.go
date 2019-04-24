// +build integration

package store_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx"
	"github.com/perangel/warp-pipe/internal/db"
	"github.com/perangel/warp-pipe/internal/store"
	"github.com/perangel/warp-pipe/pkg/config"
	"github.com/stretchr/testify/assert"
)

func setupEnv() {
	os.Setenv("DB_HOST", "127.0.0.1")
	os.Setenv("DB_PORT", "6432")
	os.Setenv("DB_NAME", "test")
	os.Setenv("DB_USER", "test")
	os.Setenv("DB_PASS", "test")
}

func getTestDBConn(config *config.Config) (*pgx.Conn, error) {
	return pgx.Connect(pgx.ConnConfig{
		Host:     config.DBConfig.DBHost,
		Port:     config.DBConfig.DBPort,
		Database: config.DBConfig.DBName,
		User:     config.DBConfig.DBUser,
		Password: config.DBConfig.DBPass,
	})
}

func insertUserSQL(firstName, lastName, email string) string {
	return fmt.Sprintf(`
		INSERT INTO users (
			first_name, 
			last_name, 
			email
		) VALUES (
			'%s', 
			'%s', 
			'%s'
		)`, firstName, lastName, email)
}

func updateUserSQL(firstName, lastName, email string) string {
	return fmt.Sprintf(`
		UPDATE users SET
			first_name = '%s', 
			last_name = '%s'
		WHERE email = '%s'`,
		firstName, lastName, email,
	)
}

func deleteUserSQL(email string) string {
	return fmt.Sprintf(`DELETE FROM users WHERE email = '%s'`, email)
}

func setupDB(conn *pgx.Conn, config *config.Config) error {
	_, err := conn.Exec(`
		CREATE TABLE users (
			id SERIAL PRIMARY KEY,
			first_name TEXT,
			last_name TEXT,
			email TEXT,
			UNIQUE (email)
		)`,
	)
	if err != nil {
		return err
	}

	return db.Prepare(conn, config.DBSchema, nil)
}

func teardownDB(conn *pgx.Conn) error {
	_, err := conn.Exec(`DROP TABLE users CASCADE`)
	if err != nil {
		return err
	}

	err = db.Teardown(conn)
	if err != nil {
		return err
	}

	return nil
}

func TestChangesetStore(t *testing.T) {
	setupEnv()
	cfg, _ := config.NewConfigFromEnv()
	conn, err := getTestDBConn(cfg)
	if err != nil {
		t.Error(err)
	}

	err = setupDB(conn, cfg)
	if err != nil {
		t.Error(err)
	}
	defer teardownDB(conn)

	testCases := []struct {
		ID        int64
		Stmt      string
		Action    string
		Schema    string
		Table     string
		OldValues map[string]interface{}
		NewValues map[string]interface{}
	}{
		{
			ID:        int64(1),
			Stmt:      insertUserSQL("Han", "Solo", "han@test.com"),
			Action:    "insert",
			Schema:    "public",
			Table:     "users",
			OldValues: nil,
			NewValues: map[string]interface{}{
				"first_name": "Han",
				"last_name":  "Solo",
				"email":      "han@test.com",
			},
		},
		{
			ID:        int64(2),
			Stmt:      insertUserSQL("Leia", "Skywalker", "leia@test.com"),
			Action:    "insert",
			Schema:    "public",
			Table:     "users",
			OldValues: nil,
			NewValues: map[string]interface{}{
				"first_name": "Leia",
				"last_name":  "Skywalker",
				"email":      "leia@test.com",
			},
		},
		{
			ID:     int64(3),
			Stmt:   deleteUserSQL("han@test.com"),
			Action: "delete",
			Schema: "public",
			Table:  "users",
			OldValues: map[string]interface{}{
				"id": int64(3),
			},
			NewValues: nil,
		},
		{
			ID:     int64(4),
			Stmt:   updateUserSQL("Leia", "Solo", "leia@test.com"),
			Action: "update",
			Schema: "public",
			Table:  "users",
			OldValues: map[string]interface{}{
				"first_name": "Leia",
				"last_name":  "Skywalker",
				"email":      "leia@test.com",
			},
			NewValues: map[string]interface{}{
				"first_name": "Leia",
				"last_name":  "Solo",
				"email":      "leia@test.com",
			},
		},
	}

	changesets := store.NewChangesetStore(conn)

	lastEvtTS := time.Now().Add(-1 * time.Minute)
	for _, tc := range testCases {
		_, err := conn.Exec(tc.Stmt)
		if err != nil {
			t.Error(err)
		}

		events, err := changesets.GetSinceTimestamp(context.Background(), lastEvtTS, 100)
		if err != nil {
			t.Error(err)
		}

		assert.Equal(t, 1, len(events))
		evt := events[0]

		if tc.Action == "insert" || tc.Action == "update" {
			assert.NotNil(t, tc.NewValues)
			var newValues map[string]interface{}
			err = json.Unmarshal(evt.NewValues, &newValues)
			if err != nil {
				t.Error(err)
			}

			for k, v := range tc.NewValues {
				assert.Equal(t, v.(string), newValues[k].(string))
			}
		} else if tc.Action == "delete" || tc.Action == "update" {
			assert.NotNil(t, tc.OldValues)
			var oldValues map[string]interface{}
			err = json.Unmarshal(evt.OldValues, &oldValues)
			if err != nil {
				t.Error(err)
			}

			expectedID := int64(tc.OldValues["id"].(int64))
			assert.Equal(t, expectedID, evt.ID)
		} else {
			t.Errorf("invalid changeset action: %s", evt.Action)
		}

		lastEvtTS = evt.Timestamp
	}

	t.Run("get since timestamp", func(t *testing.T) {
		events, err := changesets.GetSinceTimestamp(context.Background(), time.Now().Add(-1*time.Hour), 10)
		assert.NoError(t, err)
		assert.Equal(t, len(testCases), len(events))
	})

	t.Run("get since ID", func(t *testing.T) {
		events, err := changesets.GetSinceID(context.Background(), 2, 10)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(events))
	})

	t.Run("get by ID", func(t *testing.T) {
		event, err := changesets.GetByID(context.Background(), 4)
		assert.NoError(t, err)
		assert.NotNil(t, event)
	})
}
