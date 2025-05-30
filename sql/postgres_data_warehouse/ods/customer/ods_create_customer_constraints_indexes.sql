ALTER TABLE {{target_schema}}.{{target_table}} 
    ADD CONSTRAINT {{target_table}}_pkey PRIMARY KEY (id);
ALTER TABLE {{target_schema}}.{{target_table}} 
    ADD CONSTRAINT {{target_table}}_email_check CHECK (email ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$');
