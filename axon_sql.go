package warppipe

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

var regexSpace = regexp.MustCompile(`\s+`)

func removeDuplicateSpaces(in string) string {
	return strings.TrimSpace(regexSpace.ReplaceAllString(in, " "))
}

func (a *Axon) prepareQueryArgs(changesetCols []*ChangesetColumn) ([]string, []string, map[string]interface{}, error) {
	var cols []string
	var colArgs []string
	values := make(map[string]interface{}, len(cols))
	for _, c := range changesetCols {
		t := reflect.TypeOf(c.Value)
		if t != nil && t.Kind() == reflect.Map {
			// Found a hashmap, this is a JSON/B field. This type is not supported
			// since re-marshaling breaks md5 checksum validation. Instead pass the
			// original raw json as a string.
			return nil, nil, nil, fmt.Errorf("expected raw json string")
		}
		if t != nil && t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Interface {
			// Set empty slices to pq.Array(nil) to avoid package sql error on an
			// empty character varying[]: "unsupported type []interface {}, a slice of
			// interface"
			if reflect.ValueOf(c.Value).Len() == 0 {
				c.Value = []byte("{}")
			} else {
				c.Value = pq.Array(c.Value)
			}
		}

		if c.Value != nil && c.Type == "bytea" {
			preparedVal := fmt.Sprintf("'%s'", c.Value.(string))
			colArgs = append(colArgs, preparedVal)
			c.Value = preparedVal
		} else {
			colArgs = append(colArgs, fmt.Sprintf(":%s", c.Column))
		}

		cols = append(cols, c.Column)
		values[c.Column] = c.Value

	}

	return cols, colArgs, values, nil
}

func (a *Axon) preparePrimaryKeyWhereClause(table string, primaryKey []string) string {
	clauses := make([]string, len(primaryKey))
	for i, c := range primaryKey {
		clauses[i] = fmt.Sprintf(`"%s".%s = :%s`, table, c, c)
	}

	return strings.Join(clauses, " AND ")
}

func (a *Axon) prepareInsertQuery(change *Changeset) (string, map[string]interface{}) {
	cols, colArgs, values, err := a.prepareQueryArgs(change.NewValues)
	if err != nil {
		// TODO: Is failure the best option here? Probably no way to safely save anything.
		a.Logger.Fatalf("prepareQueryArgs: error in changeset %s: %s", change, err)
	}

	sql := fmt.Sprintf(
		`INSERT INTO "%s"."%s" (%s) VALUES (%s)`,
		change.Schema,
		change.Table,
		strings.Join(cols, ","),
		strings.Join(colArgs, ","),
	)

	return sql, values
}

func (a *Axon) prepareUpdateQuery(primaryKey []string, change *Changeset) (string, map[string]interface{}) {
	cols, colArgs, values, err := a.prepareQueryArgs(change.NewValues)
	if err != nil {
		a.Logger.Fatalf("prepareQueryArgs: error in changeset %s: %s", change, err)
	}
	setClauses := make([]string, len(cols))
	for i, c := range cols {
		setClauses[i] = fmt.Sprintf("%s = %s", c, colArgs[i])
	}

	primaryKeyWhereClauses := make([]string, len(primaryKey))
	for i, c := range primaryKey {
		primaryKeyWhereClauses[i] = fmt.Sprintf(`"%s".%s = :%s`, change.Table, c, c)
	}

	sql := fmt.Sprintf(`
		INSERT INTO "%s"."%s" (%s) VALUES (%s)
			ON CONFLICT (%s)
			DO UPDATE SET %s WHERE %s`,
		change.Schema,
		change.Table,
		strings.Join(cols, ", "),
		strings.Join(colArgs, ", "),
		strings.Join(primaryKey, ", "),
		strings.Join(setClauses, ", "),
		a.preparePrimaryKeyWhereClause(change.Table, primaryKey),
	)

	return sql, values
}

func (a *Axon) prepareDeleteQuery(primaryKey []string, change *Changeset) (string, map[string]interface{}) {
	_, _, values, err := a.prepareQueryArgs(change.OldValues)
	if err != nil {
		a.Logger.Fatalf("prepareQueryArgs: error in changeset %s: %s", change, err)
	}

	sql := fmt.Sprintf(
		`DELETE FROM "%s"."%s" WHERE %s`,
		change.Schema,
		change.Table,
		a.preparePrimaryKeyWhereClause(change.Table, primaryKey),
	)

	return sql, values
}

func (a *Axon) insertRow(sourceDB *sqlx.DB, targetDB *sqlx.DB, change *Changeset, failOnDuplicate bool) error {
	query, args := a.prepareInsertQuery(change)
	_, err := targetDB.NamedExec(query, args)
	if err != nil {
		// PG error codes: https://www.postgresql.org/docs/9.2/errcodes-appendix.html
		pqe, ok := err.(*pq.Error)
		if !ok {
			return fmt.Errorf("failed to insert %s for query %s args %s: %+v", change, removeDuplicateSpaces(query), args, err)
		}
		if pqe.Code.Name() == "unique_violation" {
			// Ignore duplicates or crash
			if failOnDuplicate {
				return fmt.Errorf("duplicate row insert failed %s", change)
			}

			a.Logger.Printf("duplicate row insert skipped %s", change)
			// Always update, even on duplicate row.
			err = a.updateColumnSequence(targetDB, change.Table, change.NewValues)
			if err != nil {
				return err
			}

			return nil
		}
		return fmt.Errorf("PG error %s:%s failed to insert %s for query %s args %s: %+v", pqe.Code, pqe.Code.Name(), change, removeDuplicateSpaces(query), args, err)
	}

	err = a.updateColumnSequence(targetDB, change.Table, change.NewValues)
	if err != nil {
		return err
	}

	err = a.updateOrphanSequences(sourceDB, targetDB, change.Table, change.NewValues)
	if err != nil {
		return err
	}

	// NOTE: row insert/update/delete logs have been updated to be the same length
	// to align timestamps to ease viewing.
	a.Logger.WithField("time-delta", time.Now().Sub(change.Timestamp)).Printf("row insert: %s", change)
	return nil
}

func (a *Axon) updateRow(targetDB *sqlx.DB, change *Changeset, primaryKey []string) error {
	query, args := a.prepareUpdateQuery(primaryKey, change)
	_, err := targetDB.NamedExec(query, args)
	if err != nil {
		pqe, ok := err.(*pq.Error)
		if !ok {
			return fmt.Errorf("failed to update %s for query %s args %s: %+v", change, removeDuplicateSpaces(query), args, err)
		}
		if pqe.Code.Name() == "unique_violation" {
			// Ignore duplicates
			a.Logger.Print("update duplicate row skipped")
			return nil
		}

		return fmt.Errorf("PG error %s:%s failed to update %s for query %s args %s: %+v", pqe.Code, pqe.Code.Name(), change, removeDuplicateSpaces(query), args, err)
	}
	a.Logger.WithField("time-delta", time.Now().Sub(change.Timestamp)).Printf("row update: %s", change)
	return nil
}

func (a *Axon) deleteRow(targetDB *sqlx.DB, change *Changeset, primaryKey []string) error {
	query, values := a.prepareDeleteQuery(primaryKey, change)
	_, err := targetDB.NamedExec(query, values)
	if err != nil {
		pqe, ok := err.(*pq.Error)
		if !ok {
			return fmt.Errorf("failed to delete %s for query %s: %+v", change, removeDuplicateSpaces(query), err)
		}
		return fmt.Errorf("PG error %s:%s delete to update %s for query %s: %+v", pqe.Code, pqe.Code.Name(), change, removeDuplicateSpaces(query), err)
	}
	a.Logger.WithField("time-delta", time.Now().Sub(change.Timestamp)).Printf("row delete: %s", change)
	return nil
}
