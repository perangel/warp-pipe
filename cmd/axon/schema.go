package main

import (
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	warppipe "github.com/perangel/warp-pipe"
)

// maps primary key columns by table
var primaryKeys = make(map[string][]string)

func checkTargetVersion(conn *sqlx.DB) error {
	var serverVersion string
	err := conn.Get(&serverVersion, "SHOW server_version;")
	if err != nil {
		return err
	}

	version := strings.Split(serverVersion, ".")
	// TODO: More thorough version checks
	major, err := strconv.Atoi(version[0])
	if err != nil {
		return err
	}
	minor, err := strconv.Atoi(version[1])
	if err != nil {
		return err
	}
	if major == 9 && minor < 5 {
		// <9.5 does not support ON CONFLICT used in row update.

		// TODO: We should be able to support older pre-upsert versions but it's
		// just going to be a slightly different strategy. We could eat the cost of
		// a read-compare-update, or alternatively we can blindly do the insert and
		// handle the conflict as our cue to update.
		return fmt.Errorf("Target DB Unsupported Version: %s", serverVersion)
	}
	log.Printf("Target DB Version: %s", serverVersion)
	return nil
}

func loadPrimaryKeys(conn *sqlx.DB) error {
	var rows []struct {
		TableName  string         `db:"table_name"`
		PrimaryKey pq.StringArray `db:"primary_key"`
	}
	err := conn.Select(&rows, `
		SELECT
			t.table_name,
			string_to_array(string_agg(c.column_name, ','), ',') AS primary_key
		FROM information_schema.key_column_usage AS c
			LEFT JOIN information_schema.table_constraints AS t
				ON c.constraint_name = t.constraint_name
				AND t.constraint_type = 'PRIMARY KEY'
		WHERE t.table_name != ''
		GROUP BY t.table_name`,
	)
	if err != nil {
		return err
	}

	for _, r := range rows {
		primaryKeys[r.TableName] = r.PrimaryKey
	}

	return nil
}

func getPrimaryKeyForChange(change *warppipe.Changeset) ([]string, error) {
	col, ok := primaryKeys[change.Table]
	if !ok {
		return nil, fmt.Errorf("no primary key in mapping for table `%s`", change.Table)
	}
	return col, nil
}
