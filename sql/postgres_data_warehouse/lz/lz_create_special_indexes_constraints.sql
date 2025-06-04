-- lz.customer
-- clean_pattern = ([[:cntrl:]]|Â| )+
CREATE INDEX IF NOT EXISTS idx_customer_id_first_name_last_name
    ON lz.customer (id, 
            TRIM(REGEXP_REPLACE(first_name, '([[:cntrl:]]|Â| )+', '', 'g')), 
            TRIM(REGEXP_REPLACE(last_name, '([[:cntrl:]]|Â| )+', '', 'g'))
        );

-- lz.salesorder
CREATE INDEX IF NOT EXISTS
    idx_salesorder_id_modified_at ON lz.salesorder (id, modified_at DESC);
CREATE INDEX IF NOT EXISTS 
    idx_salesorder_order_number_modified_at ON lz.salesorder (order_number, modified_at DESC);

-- lz.salesorderitem
CREATE INDEX IF NOT EXISTS idx_salesorderitem_item_id_modified_at 
    ON lz.salesorderitem (item_id, modified_at DESC);
CREATE INDEX IF NOT EXISTS idx_salesorderitem_product_id_modified_at_item_id 
    ON lz.salesorderitem (product_id, modified_at DESC, item_id);
CREATE INDEX IF NOT EXISTS idx_salesorderitem_product_sku_modified_at_item_id 
    ON lz.salesorderitem (product_sku, modified_at DESC, item_id);
CREATE INDEX IF NOT EXISTS idx_salesorderitem_order_id 
    ON lz.salesorderitem (order_id);
