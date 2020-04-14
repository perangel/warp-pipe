package main

import (
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	warppipe "github.com/perangel/warp-pipe"
)

// maps primary key columns by table
var primaryKeys = make(map[string][]string)

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
