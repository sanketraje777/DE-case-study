-- Check 1: Row counts
SELECT '{{target_table}}' AS table_name, COUNT(*) AS row_count FROM {{target_schema}}.{{target_table}};

-- Check 2: Critical columns NOT NULL
SELECT COUNT(*) AS {{target_table}}_product_sku_nulls FROM {{target_schema}}.{{target_table}} WHERE product_sku IS NULL;
SELECT COUNT(*) AS {{target_table}}_product_name_nulls FROM {{target_schema}}.{{target_table}} WHERE product_name IS NULL;

-- Check 4: Unique constraints
SELECT product_sku, COUNT(*) FROM {{target_schema}}.{{target_table}} GROUP BY product_sku HAVING COUNT(*) > 1;
