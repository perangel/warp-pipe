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
var sequenceColumns = make(map[string]string)

// lists sequences not associated with any table column
var orphanSequences []string

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

func printSourceStats(conn *sqlx.DB) error {
	var changesetCount int
	err := conn.QueryRow("SELECT count(id) FROM warp_pipe.changesets").Scan(&changesetCount)
	if err != nil {
		return err
	}
	log.Printf("Changesets Found in Source: %d", changesetCount)
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
		var setVal string
		err := conn.QueryRow(`
		SELECT
			setval(
				$1,
				$2,
				true
			)
		`, sequenceName, c.Value).Scan(&setVal)
		if err != nil {
			return fmt.Errorf("updateSerialColumns: %w", err)
		}
		log.Printf("sequence set %s: %s", sequenceName, setVal)
	}
	return nil
}

// loadUnconnectedSequences loads all sequences in the source database not
// associated with a table column so they can be automatically updated each
// INSERT. There is no way to watch sequence value updates, so all must be
// updated each insert.
func loadOrphanSequences(conn *sqlx.DB) error {
	var rows []struct {
		SequenceName string `db:"sequence_name"`
	}
	err := conn.Select(&rows, `
		SELECT sequence_name
		FROM information_schema.sequences
		WHERE sequence_schema = 'public'`,
	)
	if err != nil {
		return fmt.Errorf("loadOrphanSequences: %w", err)
	}

	var connectedSeq = make(map[string]bool)
	for _, seq := range columnSequences {
		connectedSeq[seq] = true
	}

	for _, r := range rows {
		if _, ok := connectedSeq[r.SequenceName]; !ok {
			// store name when not in the connected list
			orphanSequences = append(orphanSequences, r.SequenceName)
		}
	}
	log.Printf("orphaned sequences found: %v", orphanSequences)
	return nil
}

func updateOrphanSequences(sourceDB *sqlx.DB, targetDB *sqlx.DB, table string, columns []*warppipe.ChangesetColumn) error {
	for _, sequenceName := range orphanSequences {
		var lastVal int64 // PG bigint is 8bit

		err := sourceDB.QueryRow(`
		SELECT
			last_value
		FROM ` + sequenceName,
		).Scan(&lastVal)
		if err != nil {
			return fmt.Errorf("updateOrphanSequences: error getting last_value for %s: %w", sequenceName, err)
		}

		var setVal string
		err = targetDB.QueryRow(`
		SELECT
			setval(
				$1,
				$2,
				true
			)
		`, sequenceName, lastVal).Scan(&setVal)
		if err != nil {
			return fmt.Errorf("updateOrphanSequences: error setting value for %s: %w", sequenceName, err)
		}
		log.Printf("orphan sequence set %s: %v", sequenceName, setVal)
	}
	return nil
}
