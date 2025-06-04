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
  FROM {{source_schema}}.{{source_table}}
  WHERE product_id IS NOT NULL
    AND product_sku IS NOT NULL
    AND product_name IS NOT NULL
    AND TRIM(REGEXP_REPLACE(product_sku, clean_pattern, '', 'g'))::VARCHAR(50) <> ''
    AND TRIM(REGEXP_REPLACE(product_name, clean_pattern, '', 'g'))::VARCHAR(255) <> ''
  ORDER BY product_id, modified_at DESC
  LIMIT {{outer_limit}}
  OFFSET {{outer_offset}}
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
INSERT INTO {{target_schema}}.{{target_table}} (
  product_id, product_sku, product_name
)
SELECT
  product_id,
  product_sku,
  product_name
FROM latest_per_product_sku;

END $$;
