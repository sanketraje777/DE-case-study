CREATE INDEX IF NOT EXISTS 
    idx_customer_id ON lz.customer (id);

CREATE INDEX IF NOT EXISTS
    idx_salesorder_id_modified_at ON lz.salesorder (id, modified_at DESC);
CREATE INDEX IF NOT EXISTS 
    idx_salesorder_order_number_modified_at ON lz.salesorder (order_number, modified_at DESC);

-- CREATE INDEX IF NOT EXISTS 
--     idx_salesorderitem_order_id ON lz.salesorderitem (order_id);
CREATE INDEX IF NOT EXISTS idx_salesorderitem_item_id_modified_at 
    ON lz.salesorderitem (item_id, modified_at DESC);
CREATE INDEX IF NOT EXISTS idx_salesorderitem_product_id_modified_at 
    ON lz.salesorderitem (product_id, modified_at DESC);
CREATE INDEX idx_salesorderitem_product_sku_modified_at 
    ON lz.salesorderitem (product_sku, modified_at DESC);
