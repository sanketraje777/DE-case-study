ALTER TABLE {{target_schema}}.{{target_table}} 
    ADD CONSTRAINT {{target_table}}_pkey PRIMARY KEY (id);
ALTER TABLE {{target_schema}}.{{target_table}} 
    ADD CONSTRAINT {{target_table}}_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES {{target_schema}}.customer(id) 
        ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE {{target_schema}}.{{target_table}} 
    ADD CONSTRAINT {{target_table}}_order_number_key UNIQUE (order_number);
-- Create indexes for salesorder
CREATE INDEX IF NOT EXISTS 
    idx_{{target_table}}_customer_id ON {{target_schema}}.{{target_table}}(customer_id);
CREATE INDEX IF NOT EXISTS
    idx_{{target_table}}_modified_at ON {{target_schema}}.{{target_table}}(modified_at);
