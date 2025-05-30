DO $$
BEGIN
    BEGIN
        ALTER TABLE dm.sales_order_item_flat 
            ADD CONSTRAINT sales_order_item_flat_pkey PRIMARY KEY (order_id, item_id);
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error adding sales_order_item_flat_pkey, skipping...';
    END;

    BEGIN
        ALTER TABLE dm.sales_order_item_flat 
            ADD CONSTRAINT sales_order_item_flat_order_total_check CHECK (order_total >= 0);
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error adding sales_order_item_flat_order_total_check, skipping...';
    END;

    BEGIN
        ALTER TABLE dm.sales_order_item_flat 
            ADD CONSTRAINT sales_order_item_flat_total_qty_ordered_check CHECK (total_qty_ordered >= 0);
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error adding sales_order_item_flat_total_qty_ordered_check, skipping...';
    END;

    BEGIN
        ALTER TABLE dm.sales_order_item_flat 
            ADD CONSTRAINT sales_order_item_flat_customer_gender_check CHECK (customer_gender IN ('Female', 'Male'));
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error adding sales_order_item_flat_customer_gender_check, skipping...';
    END;

    BEGIN
        ALTER TABLE dm.sales_order_item_flat
            ADD CONSTRAINT sales_order_item_flat_customer_email_check CHECK (customer_email ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$');
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error adding sales_order_item_flat_customer_email_check, skipping...';
    END;

    BEGIN
        ALTER TABLE dm.sales_order_item_flat 
            ADD CONSTRAINT sales_order_item_flat_item_price_check CHECK (item_price >= 0);
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error adding sales_order_item_flat_item_price_check, skipping...';
    END;

    BEGIN
        ALTER TABLE dm.sales_order_item_flat 
            ADD CONSTRAINT sales_order_item_flat_item_qty_order_check CHECK (item_qty_order >= 0);
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error adding sales_order_item_flat_item_qty_order_check, skipping...';
    END;

    BEGIN
        ALTER TABLE dm.sales_order_item_flat 
            ADD CONSTRAINT sales_order_item_flat_item_unit_total_check CHECK (item_unit_total >= 0);
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error adding sales_order_item_flat_item_unit_total_check, skipping...';
    END;
END
$$; 

ALTER TABLE dm.sales_order_item_flat
    ALTER COLUMN item_id SET NOT NULL,
    ALTER COLUMN order_id SET NOT NULL,
    ALTER COLUMN order_number SET NOT NULL,
    ALTER COLUMN order_created_at SET NOT NULL,
    ALTER COLUMN order_total SET NOT NULL,
    ALTER COLUMN total_qty_ordered SET NOT NULL,
    ALTER COLUMN customer_id SET NOT NULL,
    ALTER COLUMN customer_name SET NOT NULL,
    ALTER COLUMN customer_email SET NOT NULL,
    ALTER COLUMN product_id SET NOT NULL,
    ALTER COLUMN product_sku SET NOT NULL,
    ALTER COLUMN product_name SET NOT NULL,
    ALTER COLUMN item_price SET NOT NULL,
    ALTER COLUMN item_qty_order SET NOT NULL,
    ALTER COLUMN item_unit_total SET NOT NULL;

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
