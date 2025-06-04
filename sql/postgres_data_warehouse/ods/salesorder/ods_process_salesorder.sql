DO $$
DECLARE
  clean_pattern TEXT := '{{var_text_clean_regex}}';  -- ([[:cntrl:]]|Â| )+ regex pattern to remove unwanted characters
BEGIN

WITH latest_per_id AS NOT MATERIALIZED (
  SELECT DISTINCT ON (id)
    id,
    customer_id,
    TRIM(REGEXP_REPLACE(order_number, clean_pattern, '', 'g')) AS order_number,
    created_at,
    modified_at,
    order_total,
    total_qty_ordered
  FROM {{source_schema}}.{{source_table}}   -- lz.salesorder
  WHERE id IS NOT NULL
    AND customer_id IS NOT NULL
    AND order_number IS NOT NULL
    AND created_at IS NOT NULL
    AND modified_at IS NOT NULL AND modified_at >= created_at
    AND order_total IS NOT NULL AND order_total >= 0
    AND total_qty_ordered IS NOT NULL AND total_qty_ordered >= 0
    AND TRIM(REGEXP_REPLACE(order_number, clean_pattern, '', 'g'))::VARCHAR(50) <> ''
  ORDER BY id, modified_at DESC
),
latest_per_order_number AS NOT MATERIALIZED (
  SELECT DISTINCT ON (order_number)
    id,
    customer_id,
    order_number::VARCHAR(50),
    created_at,
    modified_at,
    order_total::NUMERIC(10,2),
    total_qty_ordered
  FROM latest_per_id
  ORDER BY order_number, modified_at DESC
)
INSERT INTO {{target_schema}}.{{target_table}} (
  id, customer_id, order_number,
  created_at, modified_at,
  order_total, total_qty_ordered
)
SELECT
  id, customer_id, order_number,
  created_at, modified_at,
  order_total, total_qty_ordered
FROM latest_per_order_number;

END $$;
