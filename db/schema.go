package db

import (
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx"
	log "github.com/sirupsen/logrus"
)

var (
	errCreateSchema        = errors.New("error creating `warp_pipe` schema")
	errDuplicateSchema     = errors.New("`warp_pipe` schema already exists")
	errCreateTable         = errors.New("error creating `warp_pipe.changesets` table")
	errDuplicateTable      = errors.New("`warp_pipe.changesets` table already exists")
	errCreateTriggerFunc   = errors.New("error creating `on_modify` trigger function")
	errRegisterTrigger     = errors.New("error registering `on_modify` trigger on table")
	errTransactionBegin    = errors.New("error starting new transaction")
	errTransactionCommit   = errors.New("error committing transaction")
	errTransactionRollback = errors.New("error rolling back transaction")
)

// Teardown removes the `warp_pipe` schema and all associated tables and functions.
func Teardown(conn *pgx.Conn) error {
	_, err := conn.Exec("DROP SCHEMA warp_pipe CASCADE")
	if err != nil {
		return err
	}

	return nil
}

// Prepare prepares the database for capturing changesets.
// This will setup:
//     - new `warp_pipe` schema
//     - new `changesets` table in the `warp_pipe` schema
//     - new TRIGGER function to be fired AFTER an INSERT, UPDATE, or DELETE on a table
//     - registers the trigger with all configured tables in the source schema
func Prepare(conn *pgx.Conn, schemas []string, includeTables, excludeTables []string) error {
	tx, err := conn.Begin()
	if err != nil {
		return errTransactionBegin
	}

	err = createSchema(tx)
	if err != nil {
		// https://www.postgresql.org/docs/10/errcodes-appendix.html
		pgErr, ok := err.(pgx.PgError)
		if ok && pgErr.Code == "42P06" {
			return errDuplicateSchema
		}
		if ok {
			log.Printf("%v+", pgErr)
		}
		return errCreateSchema
	}

	err = createChangesetsTable(tx)
	if err != nil {
		// https://www.postgresql.org/docs/10/errcodes-appendix.html
		pgErr, ok := err.(pgx.PgError)
		if ok && pgErr.Code == "42P07" {
			return errDuplicateTable
		}
		return errCreateTable
	}

	err = createTriggerFunc(tx)
	if err != nil {
		return errCreateTriggerFunc
	}

	registerTables, err := GenerateTablesList(conn, schemas, includeTables, excludeTables)
	if err != nil {
		return err
	}

	for _, table := range registerTables {
		err = registerTrigger(tx, table)
		if err != nil {
			pgErr, ok := err.(pgx.PgError)
			if ok {
				log.Printf("%+v", pgErr)
			}
			return errRegisterTrigger
		}
	}

	if err = tx.Commit(); err != nil {
		log.WithError(err).Error(errTransactionCommit.Error())
		return errTransactionCommit
	}

	return nil
}

func createSchema(tx *pgx.Tx) error {
	_, err := tx.Exec(createSchemaWarpPipeSQL)
	if err != nil {
		return err
	}

	_, err = tx.Exec(revokeAllOnSchemaWarpPipeSQL)
	if err != nil {
		return err
	}

	_, err = tx.Exec(commentOnSchemaWarpPipeSQL)
	if err != nil {
		return err
	}

	return nil
}

func createChangesetsTable(tx *pgx.Tx) error {
	_, err := tx.Exec(createTableWarpPipeChangesetsSQL)
	if err != nil {
		return err
	}

	_, err = tx.Exec(revokeAllOnWarpPipeChangesetsSQL)
	if err != nil {
		return err
	}

	_, err = tx.Exec(createIndexChangesetsTimestampSQL)
	if err != nil {
		return err
	}

	_, err = tx.Exec(createIndexChangesetsActionSQL)
	if err != nil {
		return err
	}

	_, err = tx.Exec(createIndexChangesetsTableNameSQL)
	if err != nil {
		return err
	}

	return nil
}

func createTriggerFunc(tx *pgx.Tx) error {
	_, err := tx.Exec(createOnModifyTriggerFuncSQL)

	return err
}

// GenerateTablesList using the includes and excludes list. If no tables are specified in the includes list,
// obtain the complete list from Postgres using the supplied schemas. If any of the included tables are listed
// as excluded, remove them from the list.
func GenerateTablesList(conn *pgx.Conn, schemas, includeTables, excludeTables []string) ([]string, error) {
	tableRegister := make(map[string]bool)

	if len(includeTables) > 0 {
		for _, table := range includeTables {
			tableRegister[table] = true
		}
	} else {
		for _, schema := range schemas {
			rows, err := conn.Query(`
		SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = $1`, schema,
			)
			if err != nil {
				return nil, err
			}

			for rows.Next() {
				var t string
				err = rows.Scan(&t)
				if err != nil {
					return nil, err
				}

				table := fmt.Sprintf("%s.%s", schema, t)
				tableRegister[table] = true
			}
		}
	}

	for _, table := range excludeTables {
		if _, ok := tableRegister[table]; ok {
			tableRegister[table] = false
		}
	}

	tables := make([]string, 0)
	for table, include := range tableRegister {
		if include {
			tables = append(tables, table)
		}
	}

	return tables, nil
}

func registerTrigger(tx *pgx.Tx, table string) error {
	// trigger name is <schema>__<table>_changesets
	triggerName := strings.ReplaceAll(table, ".", "__")
	sql := fmt.Sprintf(`
		DO  
		$$  
		BEGIN  
			IF NOT EXISTS(
                 SELECT * FROM(
					 SELECT trigger_name AS name, concat_ws('.', event_object_schema, event_object_table) AS table 
					 FROM information_schema.triggers
                 ) AS triggers
				 WHERE triggers.name = '%s_changesets'
				 AND triggers.table = '%s'  
			)  
			THEN
				CREATE TRIGGER %s_changesets
				AFTER INSERT OR UPDATE OR DELETE
				ON "%s"
				FOR EACH ROW EXECUTE PROCEDURE warp_pipe.on_modify();
			END IF ;
		END;  
		$$`, triggerName, table, triggerName, table)
	_, err := tx.Exec(sql)

	return err
}
