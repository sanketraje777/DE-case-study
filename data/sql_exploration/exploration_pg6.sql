DO $$
DECLARE
  clean_pattern TEXT := '([[:cntrl:]]|Â)+';  -- regex pattern to remove unwanted characters
BEGIN

WITH latest_per_product_id AS NOT MATERIALIZED (
  SELECT DISTINCT ON (product_id)
    product_id,
    TRIM(REGEXP_REPLACE(product_sku, clean_pattern, '', 'g'))::VARCHAR(50) AS product_sku,
    TRIM(REGEXP_REPLACE(product_name, clean_pattern, '', 'g'))::VARCHAR(255) AS product_name,
    modified_at
  FROM lz.salesorderitem
  WHERE product_id IS NOT NULL
    AND product_sku IS NOT NULL
    AND product_name IS NOT NULL
    AND TRIM(REGEXP_REPLACE(product_sku, clean_pattern, '', 'g'))::VARCHAR(50) <> ''
    AND TRIM(REGEXP_REPLACE(product_name, clean_pattern, '', 'g'))::VARCHAR(255) <> ''
  ORDER BY product_id, modified_at DESC
),
latest_per_product_sku AS NOT MATERIALIZED (
  SELECT DISTINCT ON (product_sku)
    product_id,
    product_sku,
    product_name,
    modified_at
  FROM latest_per_product_id
  ORDER BY product_sku, modified_at DESC
)
INSERT INTO ods.product (
  product_id, product_sku, product_name
)
SELECT
  product_id,
  product_sku,
  product_name
FROM latest_per_product_sku;

END $$;

TRUNCATE TABLE ods.product CASCADE; 
SELECT drop_special_constraints_and_indexes('ods','salesorderitem',ARRAY['p','f','u']);
SELECT drop_special_constraints_and_indexes('ods','product',ARRAY['p','f','u']);
SELECT * FROM ods.product;
SELECT * FROM lz.salesorderitem;

ALTER TABLE ods.product 
    ADD CONSTRAINT product_pkey PRIMARY KEY (product_id);
ALTER TABLE ods.product 
    ADD CONSTRAINT product_product_sku_key UNIQUE (product_sku);
