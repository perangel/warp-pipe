package db

const (
	// Creates the warp_pipe schema
	createSchemaWarpPipeSQL = `CREATE SCHEMA IF NOT EXISTS warp_pipe`

	// Revokes all privileges from public on the warp_pipe schema
	revokeAllOnSchemaWarpPipeSQL = `REVOKE ALL ON SCHEMA warp_pipe FROM public`

	// Add a comment to the warp_pipe schema
	commentOnSchemaWarpPipeSQL = `COMMENT ON SCHEMA warp_pipe IS 'Changeset history tables and trigger functions'`

	// Create the warp_pipe.changesets table
	createTableWarpPipeChangesetsSQL = `
		CREATE TABLE IF NOT EXISTS warp_pipe.changesets (
			id BIGSERIAL PRIMARY KEY,
			ts TIMESTAMPTZ DEFAULT NOW() NOT NULL,
			action TEXT NOT NULL CHECK (action IN ('INSERT', 'UPDATE', 'DELETE')),
			schema_name TEXT NOT NULL,
			table_name TEXT NOT NULL,
			relid OID NOT NULL,
			new_values JSON,
			old_values JSON
		)`

	// Revoke all privileges from public on warp_pipe.changesets
	revokeAllOnWarpPipeChangesetsSQL = `REVOKE ALL ON warp_pipe.changesets FROM public`

	// Create an index for warp_pipe.changesets(ts)
	createIndexChangesetsTimestampSQL = `CREATE INDEX IF NOT EXISTS changesets_ts_idx ON warp_pipe.changesets (ts)`

	// Create an index for warp_pipe.changesets(action)
	createIndexChangesetsActionSQL = `CREATE INDEX IF NOT EXISTS changesets_action_idx ON warp_pipe.changesets (action)`

	// Create an index for warp_pipe.changesets(schema_name)
	createIndexChangesetsSchemaNameSQL = `CREATE INDEX IF NOT EXISTS changesets_schema_name_idx ON warp_pipe.changesets (schema_name)`

	// Create an index for warp_pipe.changesets(table_name)
	createIndexChangesetsTableNameSQL = `CREATE INDEX IF NOT EXISTS changesets_table_name_idx ON warp_pipe.changesets (table_name)`

	// Create warp_pipe.on_modify() trigger function
	createOnModifyTriggerFuncSQL = `
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
							row_to_json(NEW, true),
							row_to_json(OLD, true)
						);
						PERFORM pg_notify('warp_pipe_new_changeset', currval('warp_pipe.changesets_id_seq')::TEXT || '_' || current_timestamp::TEXT);
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
							row_to_json(OLD, true)
						);
						PERFORM pg_notify('warp_pipe_new_changeset', currval('warp_pipe.changesets_id_seq')::TEXT || '_' || current_timestamp::TEXT);
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
							row_to_json(NEW, true)
						);
						PERFORM pg_notify('warp_pipe_new_changeset', currval('warp_pipe.changesets_id_seq')::TEXT || '_' || current_timestamp::TEXT);
						RETURN NEW;
					ELSE
						RAISE WARNING '[WARP_PIPE.ON_MODIFY()] - Other action occurred: %, at %',TG_OP,NOW();
						RETURN NULL;
					END IF;

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
			SECURITY DEFINER`
)
