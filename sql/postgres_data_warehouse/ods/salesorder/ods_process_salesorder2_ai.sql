DO $$
DECLARE
  -- paging parameters
  inner_limit  INT := %(var_page_size)s;
  inner_offset INT := 0;
  outer_limit  INT := %(outer_limit)s;
  outer_offset INT := %(outer_offset)s;
  batch_limit  INT := %(var_batch_size)s;

  -- in-memory storage for the deduped page
  page_rows    %(target_schema)s.%(target_table)s%ROWTYPE[];  -- ods.salesorder%ROWTYPE[]
  total_rows   INT;
  start_idx    INT;
  end_idx      INT;
BEGIN
  WHILE inner_offset < outer_limit LOOP

    -- 1) pull & dedupe one page into a PL/pgSQL array
    WITH
    raw_page AS (
      SELECT *
      FROM %(source_schema)s.%(source_table)s   -- lz.salesorder
      WHERE id IS NOT NULL
        AND customer_id IS NOT NULL
        AND order_number IS NOT NULL
        AND created_at IS NOT NULL
        AND modified_at IS NOT NULL AND modified_at >= created_at
        AND order_total IS NOT NULL AND order_total >= 0
        AND total_qty_ordered IS NOT NULL AND total_qty_ordered >= 0
      ORDER BY id, modified_at DESC
      LIMIT inner_limit
      OFFSET inner_offset + outer_offset
    ),
    id_agg AS (
      SELECT
        id,
        ARRAY_AGG(customer_id    ORDER BY modified_at DESC)[1] AS customer_id,
        ARRAY_AGG(TRIM(order_number) ORDER BY modified_at DESC)[1] AS order_number,
        ARRAY_AGG(created_at     ORDER BY modified_at DESC)[1] AS created_at,
        MAX(modified_at)                                      AS modified_at,
        ARRAY_AGG(order_total::NUMERIC(10,2) ORDER BY modified_at DESC)[1] AS order_total,
        ARRAY_AGG(total_qty_ordered   ORDER BY modified_at DESC)[1] AS total_qty_ordered
      FROM raw_page
      GROUP BY id
    ),
    sku_ranked AS (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY order_number
          ORDER BY modified_at DESC
        ) AS rn_num
      FROM id_agg
    ),
    deduped AS (
      SELECT
        id, customer_id, order_number,
        created_at, modified_at,
        order_total, total_qty_ordered
      FROM sku_ranked
      WHERE rn_num = 1
    )
    SELECT ARRAY_AGG(d) 
      INTO page_rows
    FROM deduped d;

    -- 2) set up array‐bounds for sub‐batches
    total_rows := COALESCE(array_length(page_rows,1), 0);
    start_idx  := 1;

    -- 3) process sub‐batches in memory
    WHILE start_idx <= total_rows LOOP
      end_idx := LEAST(start_idx + batch_limit - 1, total_rows);

      INSERT INTO ods.salesorder (
        id, customer_id, order_number,
        created_at, modified_at,
        order_total, total_qty_ordered
      )
      SELECT
        (page_rows[i]).id,
        (page_rows[i]).customer_id,
        (page_rows[i]).order_number,
        (page_rows[i]).created_at,
        (page_rows[i]).modified_at,
        (page_rows[i]).order_total,
        (page_rows[i]).total_qty_ordered
      FROM generate_series(start_idx, end_idx) AS i
      ON CONFLICT (id) DO UPDATE
        SET
          customer_id       = EXCLUDED.customer_id,
          order_number      = EXCLUDED.order_number,
          modified_at       = EXCLUDED.modified_at,
          order_total       = EXCLUDED.order_total,
          total_qty_ordered = EXCLUDED.total_qty_ordered
      WHERE
        (ods.salesorder.customer_id,
         ods.salesorder.order_number,
         ods.salesorder.modified_at,
         ods.salesorder.order_total,
         ods.salesorder.total_qty_ordered)
        IS DISTINCT FROM
        (EXCLUDED.customer_id,
         EXCLUDED.order_number,
         EXCLUDED.modified_at,
         EXCLUDED.order_total,
         EXCLUDED.total_qty_ordered)
      ;

      start_idx := end_idx + 1;
    END LOOP;

    inner_offset := inner_offset + inner_limit;
  END LOOP;
END
$$;
