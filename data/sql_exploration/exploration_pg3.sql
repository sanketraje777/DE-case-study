DO $$
DECLARE
  inner_limit  INT := 10000;      -- page size
  inner_offset INT := 0;                      -- page size iterator
  outer_limit  INT := 8048;        -- chunk size
  outer_offset INT := 0;       -- chunk offset provided by task expansion
  batch_limit  INT := 1000;     -- batch size
  -- array to hold one "page" of orphan IDs
  orphans       ods.salesorderitem.item_id%TYPE[];    -- INT                         
  total_orphans INT;                           -- length of the array
  start_idx     INT;                           -- sub-batch start
  end_idx       INT;                           -- sub-batch end
BEGIN
  -- outer paging loop
  WHILE inner_offset < outer_limit LOOP
    -- 1) Fetch next page of orphan IDs into an array
    WITH orphan_page AS (
      SELECT item_id
      FROM ods.salesorderitem   -- ods.salesorderitem
      WHERE order_id   NOT IN (SELECT id         FROM ods.salesorder)
         OR product_id NOT IN (SELECT product_id FROM ods.product)
      ORDER BY item_id
      LIMIT inner_limit
      OFFSET inner_offset + outer_offset
    )
    SELECT ARRAY_AGG(item_id) 
      INTO orphans
    FROM orphan_page;
    -- if nothing in this page, break
    EXIT WHEN orphans IS NULL;
    -- 2) cache its length
    total_orphans := array_length(orphans, 1);
    start_idx     := 1;
    -- 3) loop over the array in sub-batches
    WHILE start_idx <= total_orphans LOOP
      end_idx := LEAST(start_idx + batch_limit - 1, total_orphans);
      -- delete this slice of IDs
      DELETE FROM ods.salesorderitem    -- ods.salesorderitem
      WHERE id = ANY(orphans[start_idx:end_idx]);
      start_idx := end_idx + 1;
    END LOOP;
    -- advance the outer window
    inner_offset := inner_offset + inner_limit;
  END LOOP;
END
$$;
