DO $$
DECLARE
  batch_size    INTEGER := {{batch_size}};  -- number of rows to delete per batch
  deleted_count INTEGER;
BEGIN
  LOOP
    WITH orphan_batch AS (
      SELECT i.item_id
      FROM {{target_schema}}.{{target_table}} AS i
      LEFT JOIN ods.salesorder o ON i.order_id = o.id
      LEFT JOIN ods.product     p ON i.product_id = p.product_id
      WHERE o.id IS NULL
         OR p.product_id IS NULL
      -- ORDER BY i.item_id
      LIMIT batch_size
    )
    DELETE FROM {{target_schema}}.{{target_table}} AS d
    USING orphan_batch AS ob
    WHERE d.item_id = ob.item_id
    RETURNING 1
    INTO deleted_count;

    -- If no rows were deleted in this iteration, exit the loop
    EXIT WHEN deleted_count IS NULL;
  END LOOP;
END;
$$;
