-- Check 1: Row counts
SELECT '{{target_table}}' AS table_name, COUNT(*) AS row_count FROM {{target_schema}}.{{target_table}};

-- Check 2: Critical columns NOT NULL
SELECT COUNT(*) AS {{target_table}}_order_number_nulls FROM {{target_schema}}.{{target_table}} WHERE order_number IS NULL;
SELECT COUNT(*) AS {{target_table}}_order_created_at_nulls FROM {{target_schema}}.{{target_table}} WHERE order_created_at IS NULL;
SELECT COUNT(*) AS {{target_table}}_customer_email_nulls FROM {{target_schema}}.{{target_table}} WHERE customer_email IS NULL;
SELECT COUNT(*) AS {{target_table}}_product_sku_nulls FROM {{target_schema}}.{{target_table}} WHERE product_sku IS NULL;
SELECT COUNT(*) AS {{target_table}}_product_name_nulls FROM {{target_schema}}.{{target_table}} WHERE product_name IS NULL;
SELECT COUNT(*) AS {{target_table}}_item_price_negative FROM {{target_schema}}.{{target_table}} WHERE item_price < 0;
SELECT COUNT(*) AS {{target_table}}_item_qty_order_negative FROM {{target_schema}}.{{target_table}} WHERE item_qty_order < 0;
SELECT COUNT(*) AS {{target_table}}_item_unit_total_negative FROM {{target_schema}}.{{target_table}} WHERE item_unit_total < 0;
