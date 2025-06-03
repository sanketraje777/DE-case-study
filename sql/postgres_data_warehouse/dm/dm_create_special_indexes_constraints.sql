DO $$
BEGIN
    BEGIN
        ALTER TABLE dm.sales_order_item_flat 
            ADD CONSTRAINT sales_order_item_flat_pkey PRIMARY KEY (item_id, order_id);
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error adding sales_order_item_flat_pkey, skipping...';
    END;

    BEGIN
        ALTER TABLE dm.sales_order_item_flat
            ADD CONSTRAINT sales_order_item_flat_customer_email_check 
                CHECK (customer_email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$');
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error adding sales_order_item_flat_customer_email_check, skipping...';
    END;
END
$$;

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
