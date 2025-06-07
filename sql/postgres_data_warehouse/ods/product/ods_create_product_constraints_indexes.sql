ALTER TABLE {{target_schema}}.{{target_table}} 
    ADD CONSTRAINT {{target_table}}_pkey PRIMARY KEY (product_id);
ALTER TABLE {{target_schema}}.{{target_table}} 
    ADD CONSTRAINT {{target_table}}_product_sku_key UNIQUE (product_sku);
