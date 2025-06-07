TRUNCATE TABLE {{target.schema}}.{{target.tables[0]}} 
    RESTART IDENTITY CASCADE;
SELECT drop_special_constraints_and_indexes(
    '{{target.schema}}', '{{target.tables[0]}}', ARRAY[]::text[]);
