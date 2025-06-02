CREATE OR REPLACE FUNCTION drop_all_constraints_and_indexes(schema_name TEXT, table_name TEXT)
RETURNS void AS $$
DECLARE
    r RECORD;
BEGIN
    -- Drop constraints
    FOR r IN
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = format('%I.%I', schema_name, table_name)::regclass
    LOOP
        EXECUTE format('ALTER TABLE %I.%I DROP CONSTRAINT %I CASCADE', schema_name, table_name, r.conname);
    END LOOP;

    -- Drop indexes
    FOR r IN
        SELECT indexname
        FROM pg_indexes
        WHERE schemaname = schema_name AND tablename = table_name
    LOOP
        EXECUTE format('DROP INDEX IF EXISTS %I.%I CASCADE', schema_name, r.indexname);
    END LOOP;

    RAISE NOTICE 'Constraints and indexes dropped for %.%', schema_name, table_name;
END;
$$ LANGUAGE plpgsql;
