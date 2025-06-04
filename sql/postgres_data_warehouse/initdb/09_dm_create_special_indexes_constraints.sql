CREATE INDEX IF NOT EXISTS idx_sales_order_item_flat_order_number 
    ON dm.sales_order_item_flat (order_number);
CREATE INDEX IF NOT EXISTS idx_sales_order_item_flat_customer_id 
    ON dm.sales_order_item_flat (customer_id);
CREATE INDEX IF NOT EXISTS idx_sales_order_item_flat_product_id 
    ON dm.sales_order_item_flat (product_id);
CREATE INDEX IF NOT EXISTS idx_sales_order_item_flat_order_created_at 
    ON dm.sales_order_item_flat (order_created_at);
CREATE INDEX IF NOT EXISTS idx_sales_order_item_flat_product_sku 
    ON dm.sales_order_item_flat (product_sku);
