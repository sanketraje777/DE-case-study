DO $$
BEGIN
    ALTER TABLE dm.sales_order_item_flat 
        ADD CONSTRAINT sales_order_item_flat_pkey PRIMARY KEY (order_id, item_id);
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Error adding sales_order_item_flat_pkey, skipping...';
END
$$;

ADD INDEX IF NOT EXISTS idx_sales_order_item_flat_order_number 
    ON dm.sales_order_item_flat (order_number);
ADD INDEX IF NOT EXISTS idx_sales_order_item_flat_customer_id 
    ON dm.sales_order_item_flat (customer_id);
ADD INDEX IF NOT EXISTS idx_sales_order_item_flat_product_id 
    ON dm.sales_order_item_flat (product_id);
ADD INDEX IF NOT EXISTS idx_sales_order_item_flat_order_created_at 
    ON dm.sales_order_item_flat (order_created_at);
ADD INDEX IF NOT EXISTS idx_sales_order_item_flat_product_sku 
    ON dm.sales_order_item_flat (product_sku);
