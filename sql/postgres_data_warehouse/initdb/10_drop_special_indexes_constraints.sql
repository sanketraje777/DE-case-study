CREATE OR REPLACE FUNCTION drop_special_constraints_and_indexes(
    schema_name TEXT,
    table_name TEXT,
    contypes TEXT[] DEFAULT ARRAY['p', 'f', 'u']
) RETURNS void AS
$$
DECLARE
    r RECORD;
BEGIN
    -- Drop constraints based on provided contypes
    FOR r IN
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = format('%I.%I', schema_name, table_name)::regclass
          AND contype = ANY(contypes)
    LOOP
        EXECUTE format(
            'ALTER TABLE %I.%I DROP CONSTRAINT IF EXISTS %I CASCADE',
            schema_name, table_name, r.conname
        );
    END LOOP;

    -- Drop all indexes except those backing constraints (already dropped with constraints)
    FOR r IN
        SELECT indexname
        FROM pg_indexes
        WHERE schemaname = schema_name
          AND tablename = table_name
    LOOP
        EXECUTE format(
            'DROP INDEX IF EXISTS %I.%I CASCADE',
            schema_name, r.indexname
        );
    END LOOP;
END;
$$ LANGUAGE plpgsql;
