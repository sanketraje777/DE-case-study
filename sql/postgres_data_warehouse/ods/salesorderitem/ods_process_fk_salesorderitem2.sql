DO $$
DECLARE
  inner_limit  INT := %(var_batch_size)s;     -- rows per inner batch
  inner_offset INT := 0;                      -- offset in this task’s outer slice
  outer_limit  INT := %(outer_limit)s;        -- provided by task expansion
  outer_offset INT := %(outer_offset)s;       -- provided by task expansion
BEGIN
  WHILE inner_offset < outer_limit LOOP

    -- Remove orphans (order_id not in ods.salesorder, product_id not in ods.product)
    DELETE FROM ods.salesorderitem soi
    USING (
      SELECT item_id
      FROM ods.salesorderitem
      WHERE order_id   NOT IN (SELECT id         FROM ods.salesorder)
         OR product_id NOT IN (SELECT product_id FROM ods.product)
      ORDER BY item_id
      LIMIT inner_limit
      OFFSET inner_offset + outer_offset
    ) bad
    WHERE soi.item_id = bad.item_id;

    inner_offset := inner_offset + inner_limit;
  END LOOP;
END
$$;
