package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"reflect"

	"github.com/jmoiron/sqlx"
	warppipe "github.com/perangel/warp-pipe"
)

func prepareQueryArgs(changesetCols []*warppipe.ChangesetColumn) ([]string, []string, map[string]interface{}, error) {
	var cols []string
	var colArgs []string
	values := make(map[string]interface{}, len(cols))
	for _, v := range changesetCols {
		t := reflect.TypeOf(v.Value)
		if t != nil && t.Kind() == reflect.Map {
			// Found a hashmap, this is a JSON/B field. Convert manually to string to
			// avoid package "sql" error: "unsupported type map[string]interface {}"".
			b, err := json.Marshal(v.Value)
			if err != nil {
				return cols, colArgs, values, fmt.Errorf("unable to marshal JSON field %s: %w", v.Column, err)
			}
			v.Value = string(b)
		}
		cols = append(cols, v.Column)
		colArgs = append(colArgs, fmt.Sprintf(":%s", v.Column))
		values[v.Column] = v.Value
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
	_, _, values, err := prepareQueryArgs(change.NewValues)
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

func insertRow(conn *sqlx.DB, schema string, change *warppipe.Changeset) error {
	query, args := prepareInsertQuery(schema, change)
	_, err := conn.NamedExec(query, args)
	if err != nil {
		return fmt.Errorf("failed to insert %s: %w", change, err)
	}
	log.Printf("row inserted: %s", change)
	return nil
}

func updateRow(conn *sqlx.DB, schema string, change *warppipe.Changeset, primaryKey []string) error {
	query, args := prepareUpdateQuery(schema, primaryKey, change)
	_, err := conn.NamedExec(query, args)
	if err != nil {
		return fmt.Errorf("Error: failed to update row: %v", err)
	}
	log.Printf("row updated: %s", change)
	return nil
}

func deleteRow(conn *sqlx.DB, schema string, change *warppipe.Changeset, primaryKey []string) error {
	query, values := prepareDeleteQuery(schema, primaryKey, change)
	_, err := conn.NamedExec(query, values)
	if err != nil {
		return fmt.Errorf("Error: failed to delete row: %v", err)
	}
	log.Printf("row deleted: %s", change)
	return nil
}
