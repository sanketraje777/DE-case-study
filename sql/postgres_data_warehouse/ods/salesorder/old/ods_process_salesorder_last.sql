INSERT INTO {{target_schema}}.{{target_table}} (
  id, customer_id, order_number,
  created_at, modified_at,
  order_total, total_qty_ordered
)
SELECT
  id,
  customer_id,
  TRIM(order_number)::VARCHAR(50),
  created_at,
  modified_at,
  order_total::NUMERIC(10, 2),
  total_qty_ordered
FROM (
  SELECT DISTINCT ON (order_number)
    id,
    customer_id,
    order_number,
    created_at,
    modified_at,
    order_total,
    total_qty_ordered
  FROM (
    SELECT DISTINCT ON (id)
      id,
      customer_id,
      order_number,
      created_at,
      modified_at,
      order_total,
      total_qty_ordered
    FROM {{source_schema}}.{{source_table}}
    WHERE id IS NOT NULL
      AND customer_id IS NOT NULL
      AND order_number IS NOT NULL AND TRIM(order_number) <> ''
      AND created_at IS NOT NULL
      AND modified_at IS NOT NULL AND modified_at >= created_at
      AND order_total IS NOT NULL AND order_total >= 0
      AND total_qty_ordered IS NOT NULL AND total_qty_ordered >= 0
    ORDER BY id, modified_at DESC
    LIMIT {{outer_limit}}
    OFFSET {{outer_offset}}
  ) AS latest_per_id
  ORDER BY order_number, modified_at DESC
) AS final_deduped;
