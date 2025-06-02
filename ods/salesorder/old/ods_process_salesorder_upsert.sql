DO $$
DECLARE
  inner_limit  INT := {{var_page_size}};      -- page size
  inner_offset INT := 0;                      -- page size iterator
  outer_limit  INT := {{outer_limit}};        -- chunk size
  outer_offset INT := {{outer_offset}};       -- chunk offset provided by task expansion
  batch_limit  INT := {{var_batch_size}};     -- batch size

  -- in-memory storage for the deduped page
  page_rows    {{target_table}}_row[];  -- salesorder_row
  total_rows   INT;
  start_idx    INT;
  end_idx      INT;
BEGIN
  WHILE inner_offset < outer_limit LOOP

    WITH raw_page AS (
      SELECT *
      FROM {{source_schema}}.{{source_table}}   -- lz.salesorder
      ORDER BY id, modified_at DESC
      LIMIT inner_limit
      OFFSET inner_offset + outer_offset
    ),

    cleaned_raw_page AS (
      SELECT *
      FROM raw_page
      WHERE id               IS NOT NULL
        AND customer_id      IS NOT NULL
        AND order_number     IS NOT NULL AND TRIM(order_number) <> ''
        AND created_at       IS NOT NULL
        AND modified_at      IS NOT NULL AND modified_at >= created_at
        AND order_total      IS NOT NULL AND order_total >= 0
        AND total_qty_ordered IS NOT NULL AND total_qty_ordered >= 0
    ),

    -- 1) Per-id aggregation using ARRAY_AGG to pick latest per column
    id_agg AS (
      SELECT
        id,
        (ARRAY_AGG(customer_id ORDER BY modified_at DESC))[1] AS customer_id,
        (ARRAY_AGG(TRIM(order_number) 
                          ORDER BY modified_at DESC))[1] AS order_number,
        (ARRAY_AGG(created_at ORDER BY modified_at DESC))[1] AS created_at,
        MAX(modified_at) AS modified_at,
        (ARRAY_AGG(order_total ORDER BY modified_at DESC))[1] AS order_total,
        (ARRAY_AGG(total_qty_ordered ORDER BY modified_at DESC))[1] AS total_qty_ordered
      FROM cleaned_raw_page
      GROUP BY id
    ),

    -- 2) Enforce unique order_number by keeping freshest per order_number
    order_number_ranked AS (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY order_number
          ORDER BY modified_at DESC
        ) AS rn_num
      FROM id_agg
    ),

    deduped_salesorder AS (
      SELECT id, customer_id, order_number::VARCHAR(50),
             created_at, modified_at,
             order_total::NUMERIC(10, 2), total_qty_ordered
      FROM order_number_ranked
      WHERE rn_num = 1
    )

    SELECT ARRAY_AGG(
	    ROW(
	      id, customer_id, order_number,
        created_at, modified_at,
        order_total, total_qty_ordered
	    )::{{target_table}}_row   -- salesorder_row	
    )
      INTO page_rows
    FROM deduped_salesorder d;

    -- 2) set up array‐bounds for sub‐batches
    total_rows := COALESCE(array_length(page_rows,1), 0);
    start_idx  := 1;

    -- 3) process sub‐batches in memory
    WHILE start_idx <= total_rows LOOP
      end_idx := LEAST(start_idx + batch_limit - 1, total_rows);

      INSERT INTO {{target_schema}}.{{target_table}} (    -- ods.salesorder
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
          created_at        = EXCLUDED.created_at,
          modified_at       = EXCLUDED.modified_at,
          order_total       = EXCLUDED.order_total,
          total_qty_ordered = EXCLUDED.total_qty_ordered
      WHERE
        ({{target_schema}}.{{target_table}}.customer_id,    -- ods.salesorder.customer_id
        {{target_schema}}.{{target_table}}.order_number,
        {{target_schema}}.{{target_table}}.created_at,
        {{target_schema}}.{{target_table}}.modified_at,
        {{target_schema}}.{{target_table}}.order_total,
        {{target_schema}}.{{target_table}}.total_qty_ordered)
        IS DISTINCT FROM
        (EXCLUDED.customer_id,
        EXCLUDED.order_number,
        EXCLUDED.created_at,
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
