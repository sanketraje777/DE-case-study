-- Generic hash-partition creation script for any table
-- Customize schema, base_table, partition_key, and n_partitions as needed
DO $$
DECLARE
  n_partitions INT := 16;             -- total number of hash partitions to create
  schema_name  TEXT := 'dm';         -- schema of the table
  base_table   TEXT := 'sales_order_item_flat';  -- base table name without schema
  partition_key TEXT := 'item_id, order_id';    -- column to hash on (single key) or comma-separated for composite
  i            INT;
BEGIN
  -- rename existing table if not already partitioned
  EXECUTE format('ALTER TABLE %I.%I RENAME TO %I_unp;', schema_name, base_table, base_table);

  -- create partitioned parent
  EXECUTE format(
    'CREATE TABLE %I.%I (
       LIKE %I.%I_unp INCLUDING ALL
     ) PARTITION BY HASH (%s);',
    schema_name, base_table,
    schema_name, base_table,
    partition_key
  );

  -- create child partitions
  FOR i IN 0..n_partitions-1 LOOP
    EXECUTE format(
      'CREATE TABLE IF NOT EXISTS %I.%I_p%s
         PARTITION OF %I.%I
         FOR VALUES WITH (MODULUS %s, REMAINDER %s);',
      schema_name,
      base_table,
      i,
      schema_name, base_table,
      n_partitions,
      i
    );
  END LOOP;

  -- backfill data from unpartitioned to new parent
  EXECUTE format('INSERT INTO %I.%I SELECT * FROM %I.%I_unp;', schema_name, base_table, schema_name, base_table);
  EXECUTE format('DROP TABLE %I.%I_unp;', schema_name, base_table);

  RAISE NOTICE 'Hash partitioning applied on %.% with % partitions.', schema_name, base_table, n_partitions;
END;
$$;

-- Example usage for DM table:
-- Set schema_name := 'dm', base_table := 'sales_order_item_flat', partition_key := 'order_id, item_id'.
