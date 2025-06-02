-- Check 1: Row counts
SELECT '{{target_table}}' AS table_name, COUNT(*) AS row_count FROM {{target_schema}}.{{target_table}};

-- Check 2: Critical columns NOT NULL
SELECT COUNT(*) AS {{target_table}}_order_number_nulls FROM {{target_schema}}.{{target_table}} WHERE order_number IS NULL;
SELECT COUNT(*) AS {{target_table}}_created_at_nulls FROM {{target_schema}}.{{target_table}} WHERE created_at IS NULL;
SELECT COUNT(*) AS {{target_table}}_modified_at_nulls FROM {{target_schema}}.{{target_table}} WHERE modified_at IS NULL;

-- Check 3: Foreign key integrity
SELECT COUNT(*) AS orphaned_orders FROM {{target_schema}}.{{target_table}} WHERE customer_id NOT IN (SELECT id FROM ods.customer);

-- Check 4: Unique constraints
SELECT order_number, COUNT(*) FROM {{target_schema}}.{{target_table}} GROUP BY order_number HAVING COUNT(*) > 1;
