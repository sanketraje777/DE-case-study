ALTER TABLE {{target_schema}}.{{target_table}}    -- dm.sales_order_item_flat 
    ADD CONSTRAINT {{target_table}}_pkey 
        PRIMARY KEY (item_id, order_id);
ALTER TABLE {{target_schema}}.{{target_table}}
    ADD CONSTRAINT {{target_table}}_customer_email_check 
        CHECK (customer_email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$');
CREATE INDEX IF NOT EXISTS idx_{{target_table}}_order_number 
    ON {{target_schema}}.{{target_table}} (order_number);
CREATE INDEX IF NOT EXISTS idx_{{target_table}}_customer_id 
    ON {{target_schema}}.{{target_table}} (customer_id);
CREATE INDEX IF NOT EXISTS idx_{{target_table}}_product_id 
    ON {{target_schema}}.{{target_table}} (product_id);
CREATE INDEX IF NOT EXISTS idx_{{target_table}}_order_created_at 
    ON {{target_schema}}.{{target_table}} (order_created_at);
CREATE INDEX IF NOT EXISTS idx_{{target_table}}_product_sku 
    ON {{target_schema}}.{{target_table}} (product_sku);
