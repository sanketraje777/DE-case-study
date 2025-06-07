DO $$
DECLARE
  inner_limit  INT := %(var_batch_size)s;     -- rows per inner batch
  inner_offset INT := 0;                      -- offset within this task’s outer slice
  outer_limit  INT := %(outer_limit)s;        -- provided by task expansion
  outer_offset INT := %(outer_offset)s;       -- provided by task expansion
BEGIN
  WHILE inner_offset < outer_limit LOOP

    WITH deduped_items AS (
      SELECT
        item_id,
        ARRAY_AGG(order_id ORDER BY modified_at DESC)[1] AS order_id,
        ARRAY_AGG(product_id ORDER BY modified_at DESC)[1] AS product_id,
        ARRAY_AGG(qty_ordered ORDER BY modified_at DESC)[1] AS qty_ordered,
        ARRAY_AGG(price::NUMERIC(10,2) ORDER BY modified_at DESC)[1] AS price,
        ARRAY_AGG(line_total::NUMERIC(12,2) ORDER BY modified_at DESC)[1] AS line_total,
        ARRAY_AGG(created_at ORDER BY modified_at DESC)[1] AS created_at,
        MAX(modified_at) AS modified_at
      FROM (
        SELECT *
        FROM lz.salesorderitem
        WHERE item_id      IS NOT NULL
          AND order_id     IS NOT NULL
          AND product_id   IS NOT NULL
          AND qty_ordered  IS NOT NULL   AND qty_ordered  >= 0
          AND price        IS NOT NULL   AND price        >= 0
          AND line_total   IS NOT NULL   AND line_total   >= 0
          AND created_at   IS NOT NULL
          AND modified_at  IS NOT NULL   AND modified_at >= created_at
        ORDER BY item_id
        LIMIT inner_limit
        OFFSET inner_offset + outer_offset
      ) sub
      WHERE modified_at >= created_at
      GROUP BY item_id
    )

    INSERT INTO ods.salesorderitem (
      item_id,
      order_id,
      product_id,
      qty_ordered,
      price,
      line_total,
      created_at,
      modified_at
    )
    SELECT
      item_id,
      order_id,
      product_id,
      qty_ordered,
      price,
      line_total,
      created_at,
      modified_at
    FROM deduped_items
    ON CONFLICT ON CONSTRAINT salesorderitem_pkey DO UPDATE
      SET
        order_id     = EXCLUDED.order_id,
        product_id   = EXCLUDED.product_id,
        qty_ordered  = EXCLUDED.qty_ordered,
        price        = EXCLUDED.price,
        line_total   = EXCLUDED.line_total,
        created_at   = EXCLUDED.created_at,
        modified_at  = EXCLUDED.modified_at
    WHERE
      (ods.salesorderitem.order_id,
       ods.salesorderitem.product_id,
       ods.salesorderitem.qty_ordered,
       ods.salesorderitem.price,
       ods.salesorderitem.line_total,
       ods.salesorderitem.created_at,
       ods.salesorderitem.modified_at)
      IS DISTINCT FROM
      (EXCLUDED.order_id,
       EXCLUDED.product_id,
       EXCLUDED.qty_ordered,
       EXCLUDED.price,
       EXCLUDED.line_total,
       EXCLUDED.created_at,
       EXCLUDED.modified_at)
    ;

    inner_offset := inner_offset + inner_limit;
  END LOOP;
END
$$;
