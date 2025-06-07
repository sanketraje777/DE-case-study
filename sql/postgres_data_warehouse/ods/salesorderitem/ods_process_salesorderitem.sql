DO $$
DECLARE
  clean_pattern TEXT := '{{var_text_clean_regex}}';  -- ([[:cntrl:]]|Â| )+ regex pattern to remove unwanted characters
BEGIN

  WITH deduped_items AS NOT MATERIALIZED (
    SELECT DISTINCT ON (item_id)
      item_id,
      order_id,
      product_id,
      qty_ordered,
      price::NUMERIC(10,2)    AS price,
      line_total::NUMERIC(12,2) AS line_total,
      created_at,
      modified_at
    FROM {{source_schema}}.{{source_table}}
    WHERE item_id IS NOT NULL
      AND order_id IS NOT NULL
      AND product_id IS NOT NULL
      AND qty_ordered IS NOT NULL       AND qty_ordered  >= 0
      AND price IS NOT NULL             AND price        >= 0
      AND line_total IS NOT NULL        AND line_total   >= 0
      AND created_at IS NOT NULL
      AND modified_at IS NOT NULL       AND modified_at >= created_at
    ORDER BY item_id, modified_at DESC
  )
  INSERT INTO {{target_schema}}.{{target_table}} (
    item_id, order_id, product_id, qty_ordered,
    price, line_total, created_at, modified_at
  )
  SELECT
    item_id, order_id, product_id, qty_ordered,
    price, line_total, created_at, modified_at
  FROM deduped_items;

END $$;
