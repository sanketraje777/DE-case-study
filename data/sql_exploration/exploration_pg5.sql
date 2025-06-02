DO $$
DECLARE
  clean_pattern TEXT := '([[:cntrl:]]|Â| )+';  -- regex pattern to remove unwanted characters
BEGIN

WITH cleaned AS NOT MATERIALIZED (
  SELECT
    id,
    TRIM(REGEXP_REPLACE(first_name, clean_pattern, '', 'g')) AS first_name,
    TRIM(REGEXP_REPLACE(last_name, clean_pattern, '', 'g')) AS last_name,
    INITCAP(TRIM(REGEXP_REPLACE(gender, clean_pattern, '', 'g'))) AS gender,
    LOWER(TRIM(REGEXP_REPLACE(email, clean_pattern, '', 'g'))) AS email,
    TRIM(REGEXP_REPLACE(billing_address, clean_pattern, '', 'g')) AS billing_address,
    TRIM(REGEXP_REPLACE(shipping_address, clean_pattern, '', 'g')) AS shipping_address
  FROM lz.customer
  WHERE id IS NOT NULL
    AND first_name IS NOT NULL
    AND last_name IS NOT NULL
    AND (gender IS NULL 
      OR LOWER(TRIM(REGEXP_REPLACE(gender, clean_pattern, '', 'g')))::VARCHAR(10) IN ('female', 'male'))
    AND email IS NOT NULL
    AND shipping_address IS NOT NULL
    AND TRIM(REGEXP_REPLACE(first_name, clean_pattern, '', 'g'))::VARCHAR(100) <> ''
    AND TRIM(REGEXP_REPLACE(last_name, clean_pattern, '', 'g'))::VARCHAR(100) <> ''
    AND TRIM(REGEXP_REPLACE(email, clean_pattern, '', 'g'))::VARCHAR(255) ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
    AND TRIM(REGEXP_REPLACE(shipping_address, clean_pattern, '', 'g')) <> ''
)
INSERT INTO ods.customer (
  id, first_name, last_name, gender, email, 
  billing_address, shipping_address
)
SELECT
  id, first_name, last_name, gender, email,
  billing_address, shipping_address
FROM (
  SELECT DISTINCT ON (id)
    id, 
	first_name::VARCHAR(100), 
	last_name::VARCHAR(100), 
	gender::VARCHAR(10), 
	email::VARCHAR(255), 
    billing_address, 
	shipping_address
  FROM cleaned
  ORDER BY id, (
    TRIM(REGEXP_REPLACE(first_name, clean_pattern, '', 'g')) || ' ' ||
    TRIM(REGEXP_REPLACE(last_name, clean_pattern, '', 'g'))
  )
) AS final_data;

END $$;

TRUNCATE TABLE ods.customer CASCADE; 
SELECT drop_special_constraints_and_indexes('ods','customer',ARRAY['p','f','u']);
SELECT * FROM ods.customer;
SELECT * FROM lz.customer;
ALTER TABLE ods.customer 
    ADD CONSTRAINT customer_pkey PRIMARY KEY (id);
ALTER TABLE ods.customer 
    ADD CONSTRAINT customer_email_check 
        CHECK (email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$');
CREATE INDEX IF NOT EXISTS 
    idx_customer_email ON ods.customer (email);
