package db

import (
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx"
	log "github.com/sirupsen/logrus"
)

type Table struct {
	Name       string
	Schema     string
	PKeyName   string
	PKeyFields map[int]string
}

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
		err = registerTrigger(tx, table.Schema, table.Name)
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
func GenerateTablesList(conn *pgx.Conn, schemas, includeTables, excludeTables []string) ([]Table, error) {
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

	tables := make([]Table, 0)
	for tableName, include := range tableRegister {
		if include {
			tableDetails, err := getTableDetails(conn, tableName)
			if err != nil {
				return nil, err
			}
			tables = append(tables, *tableDetails)
		}
	}

	return tables, nil
}

func getTableDetails(conn *pgx.Conn, name string) (*Table, error) {
	var table Table
	nameArr := strings.SplitN(name, ".", 2)
	if len(nameArr) > 1 {
		table.Schema = nameArr[0]
		table.Name = nameArr[1]
	} else {
		table.Schema = "public"
		table.Name = nameArr[0]
	}
	table.PKeyFields = make(map[int]string)
	rows, err := conn.Query(`
		SELECT 
			tco.constraint_name,
			kcu.ordinal_position as position,
			kcu.column_name as key_column
		from information_schema.table_constraints tco
		join information_schema.key_column_usage kcu
  			on kcu.constraint_name = tco.constraint_name
  			and kcu.constraint_schema = tco.constraint_schema
  			and kcu.constraint_name = tco.constraint_name
		where tco.constraint_type = 'PRIMARY KEY' and kcu.table_schema = $1 and  kcu.table_name =  $2
		order by kcu.table_schema,
	  		kcu.table_name,
	  		position;`, table.Schema, table.Name,
	)

	for rows.Next() {
		var constraintName, column string
		var position int
		err = rows.Scan(&constraintName, &position, &column)
		if err != nil {
			return nil, err
		}
		table.PKeyName = constraintName
		table.PKeyFields[position] = column
	}
	return &table, nil
}

func registerTrigger(tx *pgx.Tx, schema string, table string) error {
	// trigger name is <schema>__<table>_changesets
	triggerName := fmt.Sprintf("%s__%s_changesets", schema, table)
	sql := fmt.Sprintf(`
		DO  
		$$  
		BEGIN  
			IF NOT EXISTS(
                 SELECT * FROM(
					 SELECT trigger_name AS name, concat_ws('.', event_object_schema, event_object_table) AS table 
					 FROM information_schema.triggers
                 ) AS triggers
				 WHERE triggers.name = '%s'
				 AND triggers.table = '%s.%s'  
			)  
			THEN
				CREATE TRIGGER %s
				AFTER INSERT OR UPDATE OR DELETE
				ON "%s"."%s"
				FOR EACH ROW EXECUTE PROCEDURE warp_pipe.on_modify();
			END IF ;
		END;  
		$$`, triggerName, schema, table, triggerName, schema, table)
	_, err := tx.Exec(sql)

	return err
}

func PrepareForDataIntegrityChecks(conn *pgx.Conn) error {
	tx, err := conn.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin the transaction: %w", err)
	}

	pgConcatSQL := `	
	DO $$ BEGIN
		CREATE FUNCTION pg_concat(TEXT, TEXT) RETURNS TEXT as '
			BEGIN
				IF $1 ISNULL THEN
					RETURN $2;
				END if;
				RETURN $1 || $2;
			END;' LANGUAGE 'plpgsql';
		EXCEPTION
		WHEN duplicate_function THEN NULL;
	END $$;`

	_, err = tx.Exec(pgConcatSQL)
	if err != nil {
		return fmt.Errorf("failed to create the pg_concat function: %w", err)
	}

	pgConcatFinSQL := `
	DO $$ BEGIN
		CREATE FUNCTION pg_concat_fin(TEXT) RETURNS TEXT as '
		BEGIN
			IF $1 ISNULL THEN
				-- avoids returning a null string for empty tables, resulting in a null checksum.
				RETURN ''x'';
			END IF;
			RETURN $1;
		END;' LANGUAGE 'plpgsql';
		EXCEPTION
			WHEN duplicate_function THEN NULL;
	END $$;`

	_, err = tx.Exec(pgConcatFinSQL)
	if err != nil {
		return fmt.Errorf("failed to create the pg_concat_fin function: %w", err)
	}

	aggregateSQL := `
	DO $$ BEGIN
	CREATE AGGREGATE pg_concat (
		basetype = TEXT,
		sfunc = pg_concat,
		stype = TEXT,
		finalfunc = pg_concat_fin
	);
	EXCEPTION
		WHEN duplicate_function THEN NULL;
	END $$;`

	_, err = tx.Exec(aggregateSQL)
	if err != nil {
		return fmt.Errorf("failed to create the pg_concat aggregate: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit the transaction: %w", err)
	}

	return nil
}
