DO $$
DECLARE
  -- paging and batching parameters
  inner_limit  INT := {{var_page_size}};      -- page size
  inner_offset INT := 0;                      -- page size iterator
  outer_limit  INT := {{outer_limit}};        -- chunk size
  outer_offset INT := {{outer_offset}};       -- chunk offset provided by task expansion
  batch_limit  INT := {{var_batch_size}};     -- batch size

  -- in-memory storage for the deduped page
  page_rows    {{target_table}}_row[];  -- salesorderitem_row
  total_rows   INT;
  start_idx    INT;
  end_idx      INT;
BEGIN
  WHILE inner_offset < outer_limit LOOP

    WITH
    raw_page AS (
      SELECT *
      FROM {{source_schema}}.{{source_table}}   -- lz.salesorderitem
      WHERE item_id     IS NOT NULL
        AND order_id    IS NOT NULL
        AND product_id  IS NOT NULL
        AND qty_ordered IS NOT NULL   AND qty_ordered  >= 0
        AND price       IS NOT NULL   AND price        >= 0
        AND line_total  IS NOT NULL   AND line_total   >= 0
        AND created_at  IS NOT NULL
        AND modified_at IS NOT NULL   AND modified_at >= created_at
      ORDER BY item_id
      LIMIT inner_limit
      OFFSET inner_offset + outer_offset
    ),

    deduped_items AS (
      SELECT
        item_id,
        (ARRAY_AGG(order_id    ORDER BY modified_at DESC))[1] AS order_id,
        (ARRAY_AGG(product_id  ORDER BY modified_at DESC))[1] AS product_id,
        (ARRAY_AGG(qty_ordered ORDER BY modified_at DESC))[1] AS qty_ordered,
        (ARRAY_AGG(price::NUMERIC(10,2)       ORDER BY modified_at DESC))[1] AS price,
        (ARRAY_AGG(line_total::NUMERIC(12,2)  ORDER BY modified_at DESC))[1] AS line_total,
        (ARRAY_AGG(created_at   ORDER BY modified_at DESC))[1] AS created_at,
        MAX(modified_at)                                AS modified_at
      FROM raw_page
      GROUP BY item_id
    )

    SELECT ARRAY_AGG(
	    ROW(
	      item_id, order_id, product_id,
	      qty_ordered, price, line_total,
	      created_at, modified_at
	    )::{{target_table}}_row   -- salesorderitem_row	
    )
      INTO page_rows
    FROM deduped_items d;

    -- 2) set up array‐bounds for sub‐batches
    total_rows := COALESCE(array_length(page_rows,1), 0);
    start_idx  := 1;

    -- 3) process sub‐batches in memory
    WHILE start_idx <= total_rows LOOP
      end_idx := LEAST(start_idx + batch_limit - 1, total_rows);

      INSERT INTO {{target_schema}}.{{target_table}} (    -- ods.salesorderitem
        item_id, order_id, product_id,
        qty_ordered, price, line_total,
        created_at, modified_at
      )
      SELECT
        (page_rows[i]).item_id,
        (page_rows[i]).order_id,
        (page_rows[i]).product_id,
        (page_rows[i]).qty_ordered,
        (page_rows[i]).price,
        (page_rows[i]).line_total,
        (page_rows[i]).created_at,
        (page_rows[i]).modified_at
      FROM generate_series(start_idx, end_idx) AS i;

      start_idx := end_idx + 1;
    END LOOP;

    inner_offset := inner_offset + inner_limit;
  END LOOP;
END
$$;
