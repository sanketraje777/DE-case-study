-- Check 1: Row counts
SELECT 'customer' AS table_name, COUNT(*) AS row_count FROM ods.customer;
SELECT 'salesorder' AS table_name, COUNT(*) AS row_count FROM ods.salesorder;
SELECT 'product' AS table_name, COUNT(*) AS row_count FROM ods.product;
SELECT 'salesorderitem' AS table_name, COUNT(*) AS row_count FROM ods.salesorderitem;

-- Check 2: Critical columns NOT NULL
-- customer
SELECT COUNT(*) AS customer_email_nulls FROM ods.customer WHERE email IS NULL;
SELECT COUNT(*) AS customer_shipping_address_nulls FROM ods.customer WHERE shipping_address IS NULL;

-- salesorder
SELECT COUNT(*) AS salesorder_order_number_nulls FROM ods.salesorder WHERE order_number IS NULL;
SELECT COUNT(*) AS salesorder_created_at_nulls FROM ods.salesorder WHERE created_at IS NULL;
SELECT COUNT(*) AS salesorder_modified_at_nulls FROM ods.salesorder WHERE modified_at IS NULL;

-- product
SELECT COUNT(*) AS product_sku_nulls FROM ods.product WHERE product_sku IS NULL;
SELECT COUNT(*) AS product_name_nulls FROM ods.product WHERE product_name IS NULL;

-- salesorderitem
SELECT COUNT(*) AS salesorderitem_qty_ordered_negative FROM ods.salesorderitem WHERE qty_ordered < 0;
SELECT COUNT(*) AS salesorderitem_price_negative FROM ods.salesorderitem WHERE price < 0;
SELECT COUNT(*) AS salesorderitem_line_total_negative FROM ods.salesorderitem WHERE line_total < 0;

-- Check 3: Foreign key integrity
-- orphaned orders
SELECT COUNT(*) AS orphaned_orders FROM ods.salesorder WHERE customer_id NOT IN (SELECT id FROM ods.customer);

-- orphaned salesorderitem orders
SELECT COUNT(*) AS orphaned_orderitems_orders FROM ods.salesorderitem WHERE order_id NOT IN (SELECT id FROM ods.salesorder);

-- orphaned salesorderitem products
SELECT COUNT(*) AS orphaned_orderitems_products FROM ods.salesorderitem WHERE product_id NOT IN (SELECT product_id FROM ods.product);

-- Check 4: Unique constraints
-- duplicate order numbers
SELECT order_number, COUNT(*) FROM ods.salesorder GROUP BY order_number HAVING COUNT(*) > 1;

-- duplicate product_sku
SELECT product_sku, COUNT(*) FROM ods.product GROUP BY product_sku HAVING COUNT(*) > 1;
