package db

import (
	"errors"
	"fmt"

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
func Prepare(conn *pgx.Conn, schema string, excludeTables []string) error {
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

	registerTables, err := getTablesToRegister(conn, schema, excludeTables)
	if err != nil {
		return err
	}

	for _, table := range registerTables {
		log.Infof("registering trigger for table %s", table)
		err = registerTrigger(tx, table)
		if err != nil {
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
	_, err := tx.Exec(`CREATE SCHEMA warp_pipe`)
	if err != nil {
		return err
	}

	_, err = tx.Exec(`REVOKE ALL ON SCHEMA warp_pipe FROM public`)
	if err != nil {
		return err
	}

	_, err = tx.Exec(`COMMENT ON SCHEMA warp_pipe IS 'Changeset history tables and trigger functions'`)
	if err != nil {
		return err
	}

	return nil
}

func createChangesetsTable(tx *pgx.Tx) error {
	_, err := tx.Exec(`
		CREATE TABLE warp_pipe.changesets (
			id BIGSERIAL PRIMARY KEY,
			ts TIMESTAMPTZ DEFAULT NOW() NOT NULL,
			action TEXT NOT NULL CHECK (action IN ('INSERT', 'UPDATE', 'DELETE')),
			schema_name TEXT NOT NULL,
			table_name TEXT NOT NULL,
			relid OID NOT NULL,
			new_values JSON,
			old_values JSON
		)
	`)
	if err != nil {
		return err
	}

	_, err = tx.Exec(`REVOKE ALL ON warp_pipe.changesets FROM public`)
	if err != nil {
		return err
	}

	_, err = tx.Exec(`CREATE INDEX changesets_changeset_ts_idx ON warp_pipe.changesets (ts)`)
	if err != nil {
		return err
	}

	_, err = tx.Exec(`CREATE INDEX changesets_action_idx ON warp_pipe.changesets (action)`)
	if err != nil {
		return err
	}

	_, err = tx.Exec(`CREATE INDEX changesets_schema_name__table_name_idx ON warp_pipe.changesets (((schema_name || '.' || table_name)::TEXT))`)
	if err != nil {
		return err
	}

	return nil
}

func createTriggerFunc(tx *pgx.Tx) error {
	_, err := tx.Exec(`
		CREATE OR REPLACE FUNCTION warp_pipe.on_modify() 
		RETURNS TRIGGER AS $$
			BEGIN
				IF TG_WHEN <> 'AFTER' THEN
					RAISE EXCEPTION 'warp_pipe.on_modify() may only run as an AFTER trigger';
				END IF;
		
				IF (TG_OP = 'UPDATE') THEN
					INSERT INTO warp_pipe.changesets(
						id,
						ts,
						action,
						schema_name,
						table_name,
						relid,
						new_values,
						old_values
					) VALUES (
						nextval('warp_pipe.changesets_id_seq'),
						current_timestamp,
						TG_OP::TEXT,
						TG_TABLE_SCHEMA::TEXT,                
						TG_TABLE_NAME::TEXT,             
						TG_RELID,
						row_to_json(NEW),
						row_to_json(OLD)
					);
					RETURN NEW;
				ELSIF (TG_OP = 'DELETE') THEN
					INSERT INTO warp_pipe.changesets(
						id,
						ts,
						action,
						schema_name,
						table_name,
						relid,
						old_values
					) VALUES (
						nextval('warp_pipe.changesets_id_seq'),
						current_timestamp,
						TG_OP::TEXT,
						TG_TABLE_SCHEMA::TEXT,                
						TG_TABLE_NAME::TEXT,             
						TG_RELID,
						row_to_json(OLD)
					);
					RETURN OLD;
				ELSIF (TG_OP = 'INSERT') THEN
					INSERT INTO warp_pipe.changesets(
						id,
						ts,
						action,
						schema_name,
						table_name,
						relid,
						new_values
					) VALUES (
						nextval('warp_pipe.changesets_id_seq'),
						current_timestamp,
						TG_OP::TEXT, TG_TABLE_SCHEMA::TEXT,                
						TG_TABLE_NAME::TEXT,             
						TG_RELID,
						row_to_json(NEW)
					);
					RETURN NEW;
				ELSE
					RAISE WARNING '[WARP_PIPE.ON_MODIFY()] - Other action occurred: %, at %',TG_OP,NOW();
					RETURN NULL;
				END IF;
	
			PERFORM pg_notify('warp_pipe_new_changeset'::text, current_timestamp);

			EXCEPTION
				WHEN data_exception THEN
					RAISE WARNING '[WARP_PIPE.ON_MODIFY()] - UDF ERROR [DATA EXCEPTION] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
					RETURN NULL;
				WHEN unique_violation THEN
					RAISE WARNING '[WARP_PIPE.ON_MODIFY()] - UDF ERROR [UNIQUE] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
					RETURN NULL;
				WHEN OTHERS THEN
					RAISE WARNING '[WARP_PIPE.ON_MODIFY()] - UDF ERROR [OTHER] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
					RETURN NULL;
		END;
		$$ LANGUAGE plpgsql
		SECURITY DEFINER
	`)

	return err
}

func getTablesToRegister(conn *pgx.Conn, schema string, excludeTables []string) ([]string, error) {
	exclude := make(map[string]struct{})
	for _, t := range excludeTables {
		exclude[t] = struct{}{}
	}

	rows, err := conn.Query(`
		SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = $1`, schema,
	)
	if err != nil {
		return nil, err
	}

	var tables []string
	for rows.Next() {
		var t string
		err = rows.Scan(&t)
		if err != nil {
			return nil, nil
		}

		if _, ok := exclude[t]; !ok {
			tables = append(tables, t)
		}
	}

	return tables, nil
}

func registerTrigger(tx *pgx.Tx, table string) error {
	_, err := tx.Exec(fmt.Sprintf(`
		CREATE TRIGGER %s_changesets 
		AFTER 
			INSERT OR UPDATE OR DELETE 
		ON %s
		FOR EACH ROW EXECUTE PROCEDURE warp_pipe.on_modify()`, table, table),
	)

	return err
}
