DO $$
DECLARE
  inner_limit  INT := %(var_batch_size)s;     -- rows per inner batch
  inner_offset INT := 0;                      -- offset in this task’s outer slice
  outer_limit  INT := %(outer_limit)s;        -- provided by task expansion
  outer_offset INT := %(outer_offset)s;       -- provided by task expansion
BEGIN
  WHILE inner_offset < outer_limit LOOP

    -- Remove orphans (customer_id not in ods.customer)
    DELETE FROM ods.salesorder so
    USING (
      SELECT id
      FROM ods.salesorder
      WHERE customer_id NOT IN (SELECT id FROM ods.customer)
      ORDER BY id
      LIMIT inner_limit
      OFFSET inner_offset + outer_offset
    ) bad
    WHERE so.id = bad.id;

    inner_offset := inner_offset + inner_limit;
  END LOOP;
END
$$;
