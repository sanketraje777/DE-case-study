CREATE INDEX IF NOT EXISTS 
    idx_customer_id ON lz.customer (id);

CREATE INDEX IF NOT EXISTS 
    idx_salesorder_id ON lz.salesorder (id);
CREATE INDEX IF NOT EXISTS 
    idx_salesorder_modified_at ON lz.salesorder (modified_at);
CREATE INDEX IF NOT EXISTS
    idx_salesorder_id_modified_at ON lz.salesorder (id, modified_at DESC);

CREATE INDEX IF NOT EXISTS 
    idx_salesorderitem_item_id ON lz.salesorderitem (item_id);
CREATE INDEX IF NOT EXISTS 
    idx_salesorderitem_order_id ON lz.salesorderitem (order_id);
CREATE INDEX IF NOT EXISTS 
    idx_salesorderitem_product_id ON lz.salesorderitem (product_id);
CREATE INDEX IF NOT EXISTS 
    idx_salesorderitem_modified_at ON lz.salesorderitem (modified_at);
CREATE INDEX IF NOT EXISTS 
    idx_salesorderitem_product_id_modified_at ON lz.salesorderitem (product_id, modified_at DESC);
CREATE INDEX IF NOT EXISTS 
    idx_salesorderitem_item_id_modified_at ON lz.salesorderitem (item_id, modified_at DESC);
