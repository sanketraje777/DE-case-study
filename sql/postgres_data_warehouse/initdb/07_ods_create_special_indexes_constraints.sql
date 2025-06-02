CREATE INDEX IF NOT EXISTS idx_salesorderitem_order_id 
    ON ods.salesorderitem(order_id);
CREATE INDEX IF NOT EXISTS idx_salesorderitem_product_id 
    ON ods.salesorderitem(product_id);
CREATE INDEX IF NOT EXISTS idx_salesorderitem_item_id_order_id_modified_at 
    ON ods.salesorderitem(item_id, order_id, modified_at DESC);

-- Create indexes for salesorder
CREATE INDEX IF NOT EXISTS 
    idx_salesorder_customer_id ON ods.salesorder(customer_id);
CREATE INDEX IF NOT EXISTS
    idx_salesorder_modified_at ON ods.salesorder(modified_at);

CREATE INDEX IF NOT EXISTS
    idx_customer_email ON ods.customer(email);
