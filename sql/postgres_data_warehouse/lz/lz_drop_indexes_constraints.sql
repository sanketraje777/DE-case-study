DROP INDEX IF EXISTS 
    idx_customer_id CASCADE;

DROP INDEX IF EXISTS
    idx_salesorder_id_modified_at CASCADE;
DROP INDEX IF EXISTS
    idx_salesorder_order_number_modified_at CASCADE;

DROP INDEX IF EXISTS
    idx_salesorderitem_item_id_modified_at CASCADE;
DROP INDEX IF EXISTS 
    idx_salesorderitem_product_id_modified_at_item_id CASCADE;
DROP INDEX IF EXISTS 
    idx_salesorderitem_product_sku_modified_at_item_id CASCADE;
