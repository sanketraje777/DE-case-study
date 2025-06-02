-- Drop indexes for salesorderitem
DROP INDEX IF EXISTS ods.idx_salesorderitem_order_id CASCADE;
DROP INDEX IF EXISTS ods.idx_salesorderitem_product_id CASCADE;
DROP INDEX IF EXISTS ods.idx_salesorderitem_modified_at CASCADE;

-- Drop indexes for salesorder
DROP INDEX IF EXISTS ods.idx_salesorder_customer_id CASCADE;
DROP INDEX IF EXISTS ods.idx_salesorder_order_number CASCADE;

-- Drop constraints for salesorderitem
ALTER TABLE ods.salesorderitem 
    DROP CONSTRAINT IF EXISTS salesorderitem_pkey CASCADE,
    DROP CONSTRAINT IF EXISTS salesorderitem_order_id_fkey CASCADE,
    DROP CONSTRAINT IF EXISTS salesorderitem_product_id_fkey CASCADE;

-- Drop constraints for salesorder
ALTER TABLE ods.salesorder 
    DROP CONSTRAINT IF EXISTS salesorder_pkey CASCADE,
    DROP CONSTRAINT IF EXISTS salesorder_customer_id_fkey CASCADE,
    DROP CONSTRAINT IF EXISTS salesorder_order_number_key CASCADE;

-- Drop constraints for product
ALTER TABLE ods.product 
    DROP CONSTRAINT IF EXISTS product_pkey CASCADE,
    DROP CONSTRAINT IF EXISTS product_product_sku_key CASCADE;

-- Drop constraints for customer
ALTER TABLE ods.customer 
    DROP CONSTRAINT IF EXISTS customer_pkey CASCADE;
