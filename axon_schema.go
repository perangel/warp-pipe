package warppipe

import (
	"fmt"

	"strconv"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

// maps primary key columns by table
var primaryKeys = make(map[string][]string)

// maps serial key columns by table
var sequenceColumns = make(map[string]string)

// lists sequences not associated with any table column
var orphanSequences []string

// for each table, maps column names to their postgres data types
var columnTypes = (map[string]map[string]string{})

func (a *Axon) checkTargetVersion(conn *sqlx.DB) error {
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
	a.Logger.Printf("Target DB Version: %s", serverVersion)
	return nil
}

func (a *Axon) printStats(conn *sqlx.DB, name string) (int, error) {
	var changesetCount int
	err := conn.Get(&changesetCount, "SELECT count(id) FROM warp_pipe.changesets")
	if err != nil {
		return 0, err
	}
	a.Logger.Printf("Changesets found in %s: %d", name, changesetCount)
	return changesetCount, nil
}

func (a *Axon) loadPrimaryKeys(conn *sqlx.DB) error {
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

func (a *Axon) getPrimaryKeyForChange(change *Changeset) ([]string, error) {
	col, ok := primaryKeys[change.Table]
	if !ok {
		return nil, fmt.Errorf("no primary key in mapping for table `%s`", change.Table)
	}
	return col, nil
}

// loadColumnSequences loads sequences used explictly in a table column which
// need to be updated after INSERTs.
func (a *Axon) loadColumnSequences(conn *sqlx.DB) error {
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

		sequenceColumns[r.TableName+"/"+r.ColumnName] = sequenceName
	}
	a.Logger.Printf("sequence columns found: %v", sequenceColumns)
	return nil
}

func (a *Axon) getSequenceColumns(table, column string) (string, bool) {
	if sequenceName, ok := sequenceColumns[table+"/"+column]; ok {
		return sequenceName, true
	}
	return "", false
}

func (a *Axon) updateColumnSequence(conn *sqlx.DB, table string, columns []*ChangesetColumn) error {
	// Why no transaction? From the manual: Because sequences are
	// non-transactional, changes made by setval are not undone if the transaction
	// rolls back.
	// https://www.postgresql.org/docs/9.6/functions-sequence.html
	for _, c := range columns {
		sequenceName, ok := a.getSequenceColumns(table, c.Column)
		if !ok {
			// Column does not have a SERIAL sequence
			continue
		}
		var setVal string
		err := conn.Get(&setVal, `
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
		a.Logger.Printf("sequence set %s: %s", sequenceName, setVal)
	}
	return nil
}

// loadOrphanSequences loads all sequences in the source database not associated
// with a table column so they can be automatically updated each INSERT. There
// is no way to watch sequence value updates, so all must be updated each
// insert.
func (a *Axon) loadOrphanSequences(conn *sqlx.DB) error {
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
	for _, seq := range sequenceColumns {
		connectedSeq[seq] = true
	}

	for _, r := range rows {
		if _, ok := connectedSeq[r.SequenceName]; !ok {
			// store name when not in the connected list
			orphanSequences = append(orphanSequences, r.SequenceName)
		}
	}
	a.Logger.Printf("orphaned sequences found: %v", orphanSequences)
	return nil
}

func (a *Axon) updateOrphanSequences(sourceDB *sqlx.DB, targetDB *sqlx.DB, table string, columns []*ChangesetColumn) error {
	for _, sequenceName := range orphanSequences {
		var lastVal int64 // PG bigint is 8 bytes

		err := sourceDB.Get(&lastVal, "SELECT last_value FROM "+sequenceName)
		if err != nil {
			return fmt.Errorf("updateOrphanSequences: error getting last_value for %s: %w", sequenceName, err)
		}

		var setVal string
		err = targetDB.Get(&setVal, `
		SELECT
			setval(
				$1,
				$2,
				true
			)
		`, sequenceName, lastVal)
		if err != nil {
			return fmt.Errorf("updateOrphanSequences: error setting value for %s: %w", sequenceName, err)
		}
		a.Logger.Printf("orphan sequence set %s: %v", sequenceName, setVal)
	}
	return nil
}

// loadColumnTypes loads the data types for all the columns in the database
func loadColumnTypes(conn *sqlx.DB) error {

	var rows []struct {
		SchemaName string `db:"table_schema"`
		TableName  string `db:"table_name"`
		ColumnName string `db:"column_name"`
		DataType   string `db:"data_type"`
	}

	err := conn.Select(&rows, `
		SELECT table_schema, table_name, column_name, data_type
		FROM information_schema.columns
		WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'warp_pipe');`,
	)
	if err != nil {
		return fmt.Errorf("could not execute query to fetch column data types: %w", err)
	}

	for _, r := range rows {
		tableName := fmt.Sprintf(`"%s"."%s"`, r.SchemaName, r.TableName)
		if _, ok := columnTypes[tableName]; !ok {
			columnTypes[tableName] = make(map[string]string)
		}

		columnTypes[tableName][r.ColumnName] = r.DataType
	}

	return nil
}

// getColumnType fetches the  data type for a column in the database
func getColumnType(schema, table, column string) (string, error) {

	tableName := fmt.Sprintf(`"%s"."%s"`, schema, table)
	colType, ok := columnTypes[tableName][column]
	if !ok {
		return "", fmt.Errorf(`column type not found for column %s in table "%s"."%s"`, column, schema, table)
	}

	return colType, nil
}

// setColumnTypes updates the column data type for all columns in a changeset
func setColumnTypes(change *Changeset) error {

	for _, c := range change.OldValues {
		colType, err := getColumnType(change.Schema, change.Table, c.Column)
		if err != nil {
			return fmt.Errorf(`failed to update column type for OldValue: %w`, err)
		}

		c.Type = colType
	}

	for _, c := range change.NewValues {
		colType, err := getColumnType(change.Schema, change.Table, c.Column)
		if err != nil {
			return fmt.Errorf(`failed to update column type for NewValue: %w`, err)
		}

		c.Type = colType
	}

	return nil
}
