-- Drop indexes for salesorderitem
DROP INDEX IF EXISTS ods.idx_salesorderitem_order_id CASCADE;
DROP INDEX IF EXISTS ods.idx_salesorderitem_product_id CASCADE;
DROP INDEX IF EXISTS ods.idx_salesorderitem_modified_at CASCADE;

-- Drop indexes for salesorder
DROP INDEX IF EXISTS ods.idx_salesorder_customer_id CASCADE;
DROP INDEX IF EXISTS ods.idx_salesorder_modified_at CASCADE;

-- Drop constraints for salesorderitem
ALTER TABLE ods.salesorderitem 
    DROP CONSTRAINT IF EXISTS salesorderitem_pkey CASCADE,
    DROP CONSTRAINT IF EXISTS salesorderitem_order_id_fkey CASCADE,
    DROP CONSTRAINT IF EXISTS salesorderitem_product_id_fkey CASCADE,
    DROP CONSTRAINT IF EXISTS salesorderitem_qty_ordered_check CASCADE,
    DROP CONSTRAINT IF EXISTS salesorderitem_price_check CASCADE,
    DROP CONSTRAINT IF EXISTS salesorderitem_line_total_check CASCADE,
    DROP CONSTRAINT IF EXISTS salesorderitem_modified_at_check CASCADE;

-- Drop identity and NOT NULL from salesorderitem columns
ALTER TABLE ods.salesorderitem 
    ALTER COLUMN id DROP IDENTITY IF EXISTS,
    ALTER COLUMN order_id DROP NOT NULL,
    ALTER COLUMN product_id DROP NOT NULL,
    ALTER COLUMN qty_ordered DROP NOT NULL,
    ALTER COLUMN price DROP NOT NULL,
    ALTER COLUMN line_total DROP NOT NULL,
    ALTER COLUMN created_at DROP NOT NULL,
    ALTER COLUMN modified_at DROP NOT NULL;

-- Drop constraints for salesorder
ALTER TABLE ods.salesorder 
    DROP CONSTRAINT IF EXISTS salesorder_pkey CASCADE,
    DROP CONSTRAINT IF EXISTS salesorder_customer_id_fkey CASCADE,
    DROP CONSTRAINT IF EXISTS salesorder_order_number_key CASCADE,
    DROP CONSTRAINT IF EXISTS salesorder_order_total_check CASCADE,
    DROP CONSTRAINT IF EXISTS salesorder_total_qty_ordered_check CASCADE,
    DROP CONSTRAINT IF EXISTS salesorder_modified_at_check CASCADE;

-- Drop identity and NOT NULL from salesorder columns
ALTER TABLE ods.salesorder
    ALTER COLUMN id DROP IDENTITY IF EXISTS,
    ALTER COLUMN customer_id DROP NOT NULL,
    ALTER COLUMN order_number DROP NOT NULL,
    ALTER COLUMN created_at DROP NOT NULL,
    ALTER COLUMN modified_at DROP NOT NULL,
    ALTER COLUMN order_total DROP NOT NULL,
    ALTER COLUMN total_qty_ordered DROP NOT NULL;

-- Drop constraints for product
ALTER TABLE ods.product 
    DROP CONSTRAINT IF EXISTS product_pkey CASCADE,
    DROP CONSTRAINT IF EXISTS product_product_sku_key CASCADE;

-- Drop identity and NOT NULL from product columns
ALTER TABLE ods.product
    ALTER COLUMN id DROP IDENTITY IF EXISTS,
    ALTER COLUMN product_sku DROP NOT NULL,
    ALTER COLUMN product_name DROP NOT NULL;

-- Drop constraints for customer
ALTER TABLE ods.customer 
    DROP CONSTRAINT IF EXISTS customer_pkey CASCADE,
    DROP CONSTRAINT IF EXISTS customer_gender_check CASCADE,
    DROP CONSTRAINT IF EXISTS customer_email_check CASCADE;

-- Drop identity and NOT NULL from customer columns
ALTER TABLE ods.customer
    ALTER COLUMN id DROP IDENTITY IF EXISTS,
    ALTER COLUMN first_name DROP NOT NULL,
    ALTER COLUMN last_name DROP NOT NULL,
    ALTER COLUMN email DROP NOT NULL,
    ALTER COLUMN shipping_address DROP NOT NULL;
