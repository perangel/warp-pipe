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

// maps serial key columns by table
var columnSequences = make(map[string]string)

func checkTargetVersion(conn *sqlx.DB) error {
	var serverVersion string
	err := conn.QueryRow("SHOW server_version;").Scan(&serverVersion)
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

// loadColumnSequences loads sequences used explictly in a table column which
// need to be updated after INSERTs.
func loadColumnSequences(conn *sqlx.DB) error {
	var rows []struct {
		TableName     string `db:"table_name"`
		ColumnName    string `db:"column_name"`
		ColumnDefault string `db:"column_default"`
	}
	err := conn.Select(&rows, `
	SELECT
		table_name,
		column_name,
		column_default
	FROM information_schema.columns
	WHERE
		table_schema = 'public'
	AND
		column_default LIKE 'nextval(%'`,
	)
	if err != nil {
		return fmt.Errorf("loadColumnSequences: %w", err)
	}

	for _, r := range rows {
		colDefault := strings.Split(r.ColumnDefault, "'")
		sequenceName := colDefault[1]

		columnSequences[r.TableName+"/"+r.ColumnName] = sequenceName
	}
	log.Printf("column sequences found: %v", columnSequences)
	return nil
}

func getSequenceColumn(table, column string) (string, bool) {
	if sequenceName, ok := columnSequences[table+"/"+column]; ok {
		return sequenceName, true
	}
	return "", false
}

func updateColumnSequence(conn *sqlx.DB, table string, columns []*warppipe.ChangesetColumn) error {
	// Why no transaction? From the manual: Because sequences are
	// non-transactional, changes made by setval are not undone if the transaction
	// rolls back.
	// https://www.postgresql.org/docs/9.6/functions-sequence.html
	for _, c := range columns {
		sequenceName, ok := getSequenceColumn(table, c.Column)
		if !ok {
			// Column does not have a SERIAL sequence
			continue
		}
		var rows []struct {
			SetVal string `db:"setval"`
		}
		err := conn.Select(&rows, `
		SELECT
			setval(
				$1,
				$2,
				true
			)
		`, sequenceName, c.Value)
		if err != nil {
			return fmt.Errorf("updateSerialColumns: %w", err)
		}
		log.Printf("sequence set %s: %s", sequenceName, rows[0].SetVal)
	}
	return nil
}
