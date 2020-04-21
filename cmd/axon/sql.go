package main

import (
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"strings"

	"reflect"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	warppipe "github.com/perangel/warp-pipe"
)

var regexOneSpace = regexp.MustCompile(`\s+`)

func removeDuplicateSpaces(in string) string {
	return strings.TrimSpace(regexOneSpace.ReplaceAllString(in, " "))
}

func prepareQueryArgs(changesetCols []*warppipe.ChangesetColumn) ([]string, []string, map[string]interface{}, error) {
	var cols []string
	var colArgs []string
	values := make(map[string]interface{}, len(cols))
	for _, c := range changesetCols {
		t := reflect.TypeOf(c.Value)
		if t != nil && t.Kind() == reflect.Map {
			// Found a hashmap, this is a JSON/B field. Convert manually to string to
			// avoid package sql error: "unsupported type map[string]interface {}".
			b, err := json.Marshal(c.Value)
			if err != nil {
				return cols, colArgs, values, fmt.Errorf("unable to marshal JSON field %s: %w", c.Column, err)
			}
			c.Value = string(b)
		}
		if t != nil && t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Interface {
			// Set empty slices to pq.Array(nil) to avoid package sql error on an
			// empty character varying[]: "unsupported type []interface {}, a slice of
			// interface"
			if reflect.ValueOf(c.Value).Len() == 0 {
				c.Value = pq.Array(nil)
			}
		}
		cols = append(cols, c.Column)
		colArgs = append(colArgs, fmt.Sprintf(":%s", c.Column))
		values[c.Column] = c.Value
	}

	return cols, colArgs, values, nil
}

func preparePrimaryKeyWhereClause(table string, primaryKey []string) string {
	clauses := make([]string, len(primaryKey))
	for i, c := range primaryKey {
		clauses[i] = fmt.Sprintf("%s.%s = :%s", table, c, c)
	}

	return strings.Join(clauses, " AND ")
}

func prepareInsertQuery(schema string, change *warppipe.Changeset) (string, map[string]interface{}) {
	cols, colArgs, values, err := prepareQueryArgs(change.NewValues)
	if err != nil {
		// TODO: Is failure the best option here? Probably no way to safely save anything.
		log.Fatalf("prepareQueryArgs: error in changeset %s: %s", change, err)
	}

	sql := fmt.Sprintf(
		"INSERT INTO %s.%s (%s) VALUES (%s)",
		schema,
		change.Table,
		strings.Join(cols, ","),
		strings.Join(colArgs, ","),
	)

	return sql, values
}

func prepareUpdateQuery(schema string, primaryKey []string, change *warppipe.Changeset) (string, map[string]interface{}) {
	cols, colArgs, values, err := prepareQueryArgs(change.NewValues)
	if err != nil {
		log.Fatalf("prepareQueryArgs: error in changeset %s: %s", change, err)
	}
	setClauses := make([]string, len(cols))
	for i, c := range cols {
		setClauses[i] = fmt.Sprintf("%s = :%s", c, c)
	}

	primaryKeyWhereClauses := make([]string, len(primaryKey))
	for i, c := range primaryKey {
		primaryKeyWhereClauses[i] = fmt.Sprintf("%s.%s = :%s", change.Table, c, c)
	}

	sql := fmt.Sprintf(`
		INSERT INTO %s.%s (%s) VALUES (%s)
			ON CONFLICT (%s)
			DO UPDATE SET %s WHERE %s`,
		schema,
		change.Table,
		strings.Join(cols, ", "),
		strings.Join(colArgs, ", "),
		strings.Join(primaryKey, ", "),
		strings.Join(setClauses, ", "),
		preparePrimaryKeyWhereClause(change.Table, primaryKey),
	)

	return sql, values
}

func prepareDeleteQuery(schema string, primaryKey []string, change *warppipe.Changeset) (string, map[string]interface{}) {
	_, _, values, err := prepareQueryArgs(change.OldValues)
	if err != nil {
		log.Fatalf("prepareQueryArgs: error in changeset %s: %s", change, err)
	}

	sql := fmt.Sprintf(
		"DELETE FROM %s.%s WHERE %s",
		schema,
		change.Table,
		preparePrimaryKeyWhereClause(change.Table, primaryKey),
	)

	return sql, values
}

func insertRow(sourceDB *sqlx.DB, targetDB *sqlx.DB, schema string, change *warppipe.Changeset) error {
	query, args := prepareInsertQuery(schema, change)
	_, err := targetDB.NamedExec(query, args)
	if err != nil {
		// PG error codes: https://www.postgresql.org/docs/9.2/errcodes-appendix.html
		pqe, ok := err.(*pq.Error)
		if !ok {
			return fmt.Errorf("failed to insert %s for query %s: %+v", change, removeDuplicateSpaces(query), err)
		}
		if pqe.Code.Name() == "unique_violation" {
			// Ignore duplicates
			// TODO: Should they be updated instead?
			log.Printf("duplicate row insert skipped %s:", change)
			// Always update, even on duplicate row.
			err = updateColumnSequence(targetDB, change.Table, change.NewValues)
			if err != nil {
				return err
			}

			return nil
		}
		return fmt.Errorf("PG error %s:%s failed to insert %s for query %s: %+v", pqe.Code, pqe.Code.Name(), change, removeDuplicateSpaces(query), err)
	}

	err = updateColumnSequence(targetDB, change.Table, change.NewValues)
	if err != nil {
		return err
	}

	err = updateOrphanSequences(sourceDB, targetDB, change.Table, change.NewValues)
	if err != nil {
		return err
	}

	log.Printf("row inserted: %s", change)
	return nil
}

func updateRow(targetDB *sqlx.DB, schema string, change *warppipe.Changeset, primaryKey []string) error {
	query, args := prepareUpdateQuery(schema, primaryKey, change)
	_, err := targetDB.NamedExec(query, args)
	if err != nil {
		pqe, ok := err.(*pq.Error)
		if !ok {
			return fmt.Errorf("failed to update %s for query %s: %+v", change, removeDuplicateSpaces(query), err)
		}
		if pqe.Code.Name() == "unique_violation" {
			// Ignore duplicates
			log.Print("update duplicate row skipped")
			return nil
		}

		return fmt.Errorf("PG error %s:%s failed to update %s for query %s: %+v", pqe.Code, pqe.Code.Name(), change, removeDuplicateSpaces(query), err)
	}
	log.Printf("row updated: %s", change)
	return nil
}

func deleteRow(targetDB *sqlx.DB, schema string, change *warppipe.Changeset, primaryKey []string) error {
	query, values := prepareDeleteQuery(schema, primaryKey, change)
	_, err := targetDB.NamedExec(query, values)
	if err != nil {
		pqe, ok := err.(*pq.Error)
		if !ok {
			return fmt.Errorf("failed to delete %s for query %s: %+v", change, removeDuplicateSpaces(query), err)
		}
		return fmt.Errorf("PG error %s:%s delete to update %s for query %s: %+v", pqe.Code, pqe.Code.Name(), change, removeDuplicateSpaces(query), err)
	}
	log.Printf("row deleted: %s", change)
	return nil
}
