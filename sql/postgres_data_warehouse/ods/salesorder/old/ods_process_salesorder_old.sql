DO $$
DECLARE
  inner_limit  INT := %(var_batch_size)s;     -- rows per inner batch
  inner_offset INT := 0;                      -- offset within this task’s outer slice
  outer_limit  INT := %(outer_limit)s;        -- provided by task expansion
  outer_offset INT := %(outer_offset)s;       -- provided by task expansion
BEGIN
  WHILE inner_offset < outer_limit LOOP

    WITH deduped_orders AS (
      SELECT
        id,
        ARRAY_AGG(customer_id ORDER BY modified_at DESC)[1] AS customer_id,
        ARRAY_AGG(TRIM(order_number)::VARCHAR(50) ORDER BY modified_at DESC)[1] AS order_number,
        ARRAY_AGG(created_at ORDER BY modified_at DESC)[1] AS created_at,
        MAX(modified_at)            AS modified_at,
        ARRAY_AGG(order_total::NUMERIC(10, 2) ORDER BY modified_at DESC)[1] AS order_total,
        ARRAY_AGG(total_qty_ordered ORDER BY modified_at DESC)[1] AS total_qty_ordered
      FROM (
        SELECT *
        FROM lz.salesorder
        WHERE id IS NOT NULL
          AND customer_id IS NOT NULL
          AND order_number IS NOT NULL
          AND created_at IS NOT NULL
          AND modified_at IS NOT NULL
          AND modified_at >= created_at
          AND order_total IS NOT NULL 
          AND order_total >= 0
          AND total_qty_ordered IS NOT NULL
          AND total_qty_ordered >= 0
        ORDER BY id
        LIMIT inner_limit
        OFFSET inner_offset + outer_offset
      ) sub
      WHERE modified_at >= created_at
      GROUP BY id
    )

    INSERT INTO ods.salesorder (
      id,
      customer_id,
      order_number,
      created_at,
      modified_at,
      order_total,
      total_qty_ordered
    )
    SELECT
      id,
      customer_id,
      order_number,
      created_at,
      modified_at,
      order_total,
      total_qty_ordered
    FROM deduped_orders
    ON CONFLICT ON CONSTRAINT salesorder_pkey DO UPDATE
      SET
        customer_id       = EXCLUDED.customer_id,
        order_number      = EXCLUDED.order_number,
        created_at        = EXCLUDED.created_at,
        modified_at       = EXCLUDED.modified_at,
        order_total       = EXCLUDED.order_total,
        total_qty_ordered = EXCLUDED.total_qty_ordered
    WHERE
      (ods.salesorder.customer_id,
       ods.salesorder.order_number,
       ods.salesorder.created_at,
       ods.salesorder.modified_at,
       ods.salesorder.order_total,
       ods.salesorder.total_qty_ordered)
      IS DISTINCT FROM
      (EXCLUDED.customer_id,
       EXCLUDED.order_number,
       EXCLUDED.created_at,
       EXCLUDED.modified_at,
       EXCLUDED.order_total,
       EXCLUDED.total_qty_ordered)
    ;

    inner_offset := inner_offset + inner_limit;
  END LOOP;
END
$$;
