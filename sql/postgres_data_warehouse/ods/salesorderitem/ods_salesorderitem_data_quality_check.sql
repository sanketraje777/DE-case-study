-- Check 1: Row counts
SELECT '{{target_table}}' AS table_name, COUNT(*) AS row_count FROM {{target_schema}}.{{target_table}};

-- Check 2: Critical columns NOT NULL
SELECT COUNT(*) AS {{target_table}}_qty_ordered_negative FROM {{target_schema}}.{{target_table}} WHERE qty_ordered < 0;
SELECT COUNT(*) AS {{target_table}}_price_negative FROM {{target_schema}}.{{target_table}} WHERE price < 0;
SELECT COUNT(*) AS {{target_table}}_line_total_negative FROM {{target_schema}}.{{target_table}} WHERE line_total < 0;

-- Check 3: Foreign key integrity
SELECT COUNT(*) AS orphaned_{{target_table}}_orders FROM {{target_schema}}.{{target_table}} WHERE order_id NOT IN (SELECT id FROM ods.salesorder);
SELECT COUNT(*) AS orphaned_{{target_table}}_products FROM {{target_schema}}.{{target_table}} WHERE product_id NOT IN (SELECT product_id FROM ods.product);
