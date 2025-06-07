DO $$
DECLARE
  partitioned_schema TEXT := 'lz';                 -- Schema of the partitioned table
  partitioned_table TEXT := 'customer';  -- Replace with your table
  consolidated_table TEXT := partitioned_table || '_consolidated';
  partition_table TEXT;
BEGIN
  -- 1. Create the consolidated table in the same schema
  EXECUTE format('CREATE TABLE %I.%I AS TABLE %I.%I WITH NO DATA;', 
                  partitioned_schema, consolidated_table, partitioned_schema, partitioned_table);

  -- 2. Insert data from each partition
  FOR partition_table IN
    SELECT child.relname
    FROM pg_inherits
    JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
    JOIN pg_class child ON pg_inherits.inhrelid = child.oid
    JOIN pg_namespace nsp ON child.relnamespace = nsp.oid
    WHERE parent.relname = partitioned_table
      AND nsp.nspname = partitioned_schema
  LOOP
    RAISE NOTICE 'Copying data from partition: %', partition_table;
    EXECUTE format('INSERT INTO %I.%I SELECT * FROM %I.%I;', 
                    partitioned_schema, consolidated_table, partitioned_schema, partition_table);
  END LOOP;

  -- 3. Drop the partitioned table (including partitions)
  EXECUTE format('DROP TABLE %I.%I CASCADE;', partitioned_schema, partitioned_table);

  -- 4. Rename consolidated table to the original table name
  EXECUTE format('ALTER TABLE %I.%I RENAME TO %I;', 
                  partitioned_schema, consolidated_table, partitioned_table);

  RAISE NOTICE 'Partitioned table %.% has been replaced by its consolidated version!', partitioned_schema, partitioned_table;
END $$;
