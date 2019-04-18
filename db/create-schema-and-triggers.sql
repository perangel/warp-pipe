CREATE SCHEMA warp_pipe;
REVOKE ALL ON SCHEMA warp_pipe FROM public;

COMMENT ON SCHEMA warp_pipe 
    IS 'Changeset history tables and trigger functions';

CREATE TABLE warp_pipe.changesets (
    id BIGSERIAL PRIMARY KEY,
    ts TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    action TEXT NOT NULL CHECK (action IN ('INSERT', 'UPDATE', 'DELETE')),
    schema_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    relid OID NOT NULL,
    new_values JSON NOT NULL,
    old_values JSON
);

REVOKE ALL ON warp_pipe.changesets FROM public;

CREATE INDEX changesets_changeset_ts_idx ON warp_pipe.changesets (ts);
CREATE INDEX changesets_action_idx ON warp_pipe.changesets (action);
CREATE INDEX changesets_schema_name__table_name_idx ON warp_pipe.changesets (((schema_name || '.' || table_name)::TEXT));

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
                TG_OP::TEXT,
                TG_TABLE_SCHEMA::TEXT,                
                TG_TABLE_NAME::TEXT,             
                TG_RELID,
                row_to_json(NEW)
            );
            RETURN NEW;
        ELSE
            RAISE WARNING '[WARP_PIPE.ON_MODIFY()] - Other action occurred: %, at %',TG_OP,now();
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
SECURITY DEFINER
SET search_path = pg_catalog, warp_pipe;
