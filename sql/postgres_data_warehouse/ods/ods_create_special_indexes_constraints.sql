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
            ADD CONSTRAINT salesorderitem_product_id_fkey FOREIGN KEY (product_id) REFERENCES ods.product(id) ON DELETE CASCADE ON UPDATE CASCADE;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error adding salesorderitem_product_id_fkey, skipping...';
    END;
END $$;

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
            ADD CONSTRAINT salesorder_customer_id_fkey 
                FOREIGN KEY (customer_id) 
                REFERENCES ods.customer(id) 
                ON DELETE CASCADE ON UPDATE CASCADE;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error adding salesorder_customer_id_fkey, skipping...';
    END;

    BEGIN
        ALTER TABLE ods.salesorder 
            ADD CONSTRAINT salesorder_order_number_key 
                UNIQUE (order_number);
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error adding salesorder_order_number_key, skipping...';
    END;
END $$;

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
    BEGIN
        ALTER TABLE ods.customer 
            ADD CONSTRAINT customer_pkey PRIMARY KEY (id);
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error adding customer_pkey, skipping...';
    END;

    BEGIN
        ALTER TABLE ods.customer 
            ADD CONSTRAINT customer_email_check CHECK (email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$');
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error adding customer_email_check, skipping...';
    END;
END $$;

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
