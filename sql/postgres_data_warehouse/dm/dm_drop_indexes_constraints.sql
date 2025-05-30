DROP INDEX IF EXISTS idx_sales_order_item_flat_order_number CASCADE;
DROP INDEX IF EXISTS idx_sales_order_item_flat_customer_id CASCADE;
DROP INDEX IF EXISTS idx_sales_order_item_flat_product_id CASCADE;
DROP INDEX IF EXISTS idx_sales_order_item_flat_order_created_at CASCADE;
DROP INDEX IF EXISTS idx_sales_order_item_flat_product_sku CASCADE;

ALTER TABLE dm.sales_order_item_flat
    DROP CONSTRAINT IF EXISTS sales_order_item_flat_pkey CASCADE,
    DROP CONSTRAINT IF EXISTS sales_order_item_flat_order_total_check CASCADE,
    DROP CONSTRAINT IF EXISTS sales_order_item_flat_total_qty_ordered_check CASCADE,
    DROP CONSTRAINT IF EXISTS sales_order_item_flat_customer_gender_check CASCADE,
    DROP CONSTRAINT IF EXISTS sales_order_item_flat_customer_email_check CASCADE,
    DROP CONSTRAINT IF EXISTS sales_order_item_flat_item_price_check CASCADE,
    DROP CONSTRAINT IF EXISTS sales_order_item_flat_item_qty_order_check CASCADE,
    DROP CONSTRAINT IF EXISTS sales_order_item_flat_item_unit_total_check CASCADE;

ALTER TABLE dm.sales_order_item_flat
    ALTER COLUMN item_id DROP NOT NULL,
    ALTER COLUMN order_id DROP NOT NULL,
    ALTER COLUMN order_number DROP NOT NULL,
    ALTER COLUMN order_created_at DROP NOT NULL,
    ALTER COLUMN order_total DROP NOT NULL,
    ALTER COLUMN total_qty_ordered DROP NOT NULL,
    ALTER COLUMN customer_id DROP NOT NULL,
    ALTER COLUMN customer_name DROP NOT NULL,
    ALTER COLUMN customer_email DROP NOT NULL,
    ALTER COLUMN product_id DROP NOT NULL,
    ALTER COLUMN product_sku DROP NOT NULL,
    ALTER COLUMN product_name DROP NOT NULL,
    ALTER COLUMN item_price DROP NOT NULL,
    ALTER COLUMN item_qty_order DROP NOT NULL,
    ALTER COLUMN item_unit_total DROP NOT NULL;
