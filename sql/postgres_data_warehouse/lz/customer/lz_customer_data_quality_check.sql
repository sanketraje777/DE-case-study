-- Check 1: Row counts
SELECT '{{target_table}}' AS table_name, COUNT(*) AS row_count FROM {{target_schema}}.{{target_table}};

-- Check 2: Critical columns NOT NULL
SELECT COUNT(*) AS {{target_table}}_email_nulls FROM {{target_schema}}.{{target_table}} WHERE email IS NULL;
SELECT COUNT(*) AS {{target_table}}_shipping_address_nulls FROM {{target_schema}}.{{target_table}} WHERE shipping_address IS NULL;
