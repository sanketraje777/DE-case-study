DO $$
BEGIN
    BEGIN
        ALTER TABLE ods.salesorderitem 
            ADD CONSTRAINT salesorderitem_pkey PRIMARY KEY (item_id);
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error adding salesorderitem_pkey, skipping...';
    END;

    BEGIN
        ALTER TABLE ods.salesorderitem 
            ADD CONSTRAINT salesorderitem_order_id_fkey FOREIGN KEY (order_id) REFERENCES ods.salesorder(id) ON DELETE CASCADE ON UPDATE CASCADE;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error adding salesorderitem_order_id_fkey, skipping...';
    END;

    BEGIN
        ALTER TABLE ods.salesorderitem 
            ADD CONSTRAINT salesorderitem_product_id_fkey FOREIGN KEY (product_id) REFERENCES ods.product(product_id) ON DELETE CASCADE ON UPDATE CASCADE;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error adding salesorderitem_product_id_fkey, skipping...';
    END;

    BEGIN
        ALTER TABLE ods.salesorderitem 
            ADD CONSTRAINT salesorderitem_qty_ordered_check CHECK (qty_ordered > 0);
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error adding salesorderitem_qty_ordered_check, skipping...';
    END;

    BEGIN
        ALTER TABLE ods.salesorderitem 
            ADD CONSTRAINT salesorderitem_price_check CHECK (price >= 0);
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error adding salesorderitem_price_check, skipping...';
    END;

    BEGIN
        ALTER TABLE ods.salesorderitem 
            ADD CONSTRAINT salesorderitem_line_total_check CHECK (line_total = qty_ordered * price);
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error adding salesorderitem_line_total_check, skipping...';
    END;

    BEGIN
        ALTER TABLE ods.salesorderitem 
            ADD CONSTRAINT salesorderitem_modified_at_check CHECK (modified_at >= created_at);
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error adding salesorderitem_modified_at_check, skipping...';
    END;
END $$;

DO $$
BEGIN
    ALTER TABLE ods.salesorderitem
        ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY;
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Error adding ods.salesorderitem.id IDENTITY, skipping...';
END $$;

ALTER TABLE ods.salesorderitem 
    ALTER COLUMN order_id SET NOT NULL,
    ALTER COLUMN product_id SET NOT NULL,
    ALTER COLUMN qty_ordered SET NOT NULL,
    ALTER COLUMN price SET NOT NULL,
    ALTER COLUMN line_total SET NOT NULL,
    ALTER COLUMN created_at SET NOT NULL,
    ALTER COLUMN modified_at SET NOT NULL;

DO $$
BEGIN
    BEGIN
        ALTER TABLE ods.salesorder 
            ADD CONSTRAINT salesorder_pkey PRIMARY KEY (id);
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error adding salesorder_pkey, skipping...';
    END;

    BEGIN
        ALTER TABLE ods.salesorder 
            ADD CONSTRAINT salesorder_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES ods.customer(id) ON DELETE CASCADE ON UPDATE CASCADE;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error adding salesorder_customer_id_fkey, skipping...';
    END;

    BEGIN
        ALTER TABLE ods.salesorder 
            ADD CONSTRAINT salesorder_order_number_key UNIQUE (order_number);
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error adding salesorder_order_number_key, skipping...';
    END;

    BEGIN
        ALTER TABLE ods.salesorder 
            ADD CONSTRAINT salesorder_order_total_check CHECK (order_total >= 0);
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error adding salesorder_order_total_check, skipping...';
    END;

    BEGIN
        ALTER TABLE ods.salesorder 
            ADD CONSTRAINT salesorder_total_qty_ordered_check CHECK (total_qty_ordered >= 0);
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error adding salesorder_total_qty_ordered_check, skipping...';
    END;

    BEGIN
        ALTER TABLE ods.salesorder 
            ADD CONSTRAINT salesorder_modified_at_check CHECK (modified_at >= created_at);
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error adding salesorder_modified_at_check, skipping...';
    END;
END $$;

DO $$
BEGIN
    ALTER TABLE ods.salesorder
        ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY;
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Error adding ods.salesorder.id IDENTITY, skipping...';
END $$;

ALTER TABLE ods.salesorder
    ALTER COLUMN customer_id SET NOT NULL,
    ALTER COLUMN order_number SET NOT NULL,
    ALTER COLUMN created_at SET NOT NULL,
    ALTER COLUMN modified_at SET NOT NULL,
    ALTER COLUMN order_total SET NOT NULL,
    ALTER COLUMN total_qty_ordered SET NOT NULL;

DO $$
BEGIN
    BEGIN
        ALTER TABLE ods.product 
            ADD CONSTRAINT product_pkey PRIMARY KEY (id);
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error adding product_pkey, skipping...';
    END;

    BEGIN
        ALTER TABLE ods.product 
            ADD CONSTRAINT product_product_sku_key UNIQUE (product_sku);
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error adding product_product_sku_key, skipping...';
    END;
END $$;

DO $$
BEGIN
    ALTER TABLE ods.product
        ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY;
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Error adding ods.product.id IDENTITY, skipping...';
END $$;

ALTER TABLE ods.product
    ALTER COLUMN product_sku SET NOT NULL,
    ALTER COLUMN product_name SET NOT NULL;

DO $$
BEGIN
    BEGIN
        ALTER TABLE ods.customer 
            ADD CONSTRAINT customer_pkey PRIMARY KEY (id);
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error adding customer_pkey, skipping...';
    END;

    BEGIN
        ALTER TABLE ods.customer 
            ADD CONSTRAINT customer_gender_check CHECK (gender IN ('Female', 'Male'));
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error adding customer_gender_check, skipping...';
    END;

    BEGIN
        ALTER TABLE ods.customer 
            ADD CONSTRAINT customer_email_check CHECK (email ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$');
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error adding customer_email_check, skipping...';
    END;
END $$;

DO $$
BEGIN
    ALTER TABLE ods.customer
        ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY;
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Error adding ods.customer.id IDENTITY, skipping...';
END $$;

ALTER TABLE ods.customer
    ALTER COLUMN first_name SET NOT NULL,
    ALTER COLUMN last_name SET NOT NULL,
    ALTER COLUMN email SET NOT NULL,
    ALTER COLUMN shipping_address SET NOT NULL;

CREATE INDEX IF NOT EXISTS 
    idx_salesorderitem_order_id ON ods.salesorderitem(order_id);
CREATE INDEX IF NOT EXISTS 
    idx_salesorderitem_product_id ON ods.salesorderitem(product_id);
CREATE INDEX IF NOT EXISTS
    idx_salesorderitem_modified_at ON ods.salesorderitem(modified_at);


-- Create indexes for salesorder
CREATE INDEX IF NOT EXISTS 
    idx_salesorder_customer_id ON ods.salesorder(customer_id);
CREATE INDEX IF NOT EXISTS
    idx_salesorder_modified_at ON ods.salesorder(modified_at);
