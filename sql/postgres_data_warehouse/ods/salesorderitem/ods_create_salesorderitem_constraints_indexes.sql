ALTER TABLE {{target_schema}}.{{target_table}} 
    ADD CONSTRAINT {{target_table}}_pkey PRIMARY KEY (item_id);
ALTER TABLE {{target_schema}}.{{target_table}} 
    ADD CONSTRAINT {{target_table}}_order_id_fkey FOREIGN KEY (order_id) REFERENCES {{target_schema}}.salesorder(id) 
        ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE {{target_schema}}.{{target_table}} 
    ADD CONSTRAINT {{target_table}}_product_id_fkey FOREIGN KEY (product_id) REFERENCES {{target_schema}}.product(product_id) 
        ON DELETE CASCADE ON UPDATE CASCADE;
CREATE INDEX IF NOT EXISTS 
    idx_{{target_table}}_order_id ON {{target_schema}}.{{target_table}}(order_id);
CREATE INDEX IF NOT EXISTS 
    idx_{{target_table}}_product_id ON {{target_schema}}.{{target_table}}(product_id);
CREATE INDEX IF NOT EXISTS
    idx_{{target_table}}_modified_at ON {{target_schema}}.{{target_table}}(modified_at);
