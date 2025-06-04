DO $$
DECLARE
  batch_size    INTEGER := {{batch_size}};  -- number of rows to delete per batch
  deleted_count INTEGER;
BEGIN
  LOOP
    WITH orphan_batch AS (
      SELECT s.id
      FROM {{target_schema}}.{{target_table}} AS s
      LEFT JOIN ods.customer c ON s.customer_id = c.id
      WHERE c.id IS NULL
      -- ORDER BY s.id
      LIMIT batch_size
    )
    DELETE FROM {{target_schema}}.{{target_table}} AS tgt
    USING orphan_batch AS ob
    WHERE tgt.id = ob.id
    RETURNING 1
    INTO deleted_count;

    EXIT WHEN deleted_count IS NULL;
  END LOOP;
END;
$$;
