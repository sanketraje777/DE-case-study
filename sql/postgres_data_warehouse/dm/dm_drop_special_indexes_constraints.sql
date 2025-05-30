DROP INDEX IF EXISTS idx_sales_order_item_flat_order_number CASCADE;
DROP INDEX IF EXISTS idx_sales_order_item_flat_customer_id CASCADE;
DROP INDEX IF EXISTS idx_sales_order_item_flat_product_id CASCADE;
DROP INDEX IF EXISTS idx_sales_order_item_flat_order_created_at CASCADE;
DROP INDEX IF EXISTS idx_sales_order_item_flat_product_sku CASCADE;

ALTER TABLE dm.sales_order_item_flat
    DROP CONSTRAINT IF EXISTS sales_order_item_flat_pkey CASCADE;
