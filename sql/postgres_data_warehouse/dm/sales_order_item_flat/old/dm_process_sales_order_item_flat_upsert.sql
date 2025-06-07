-- The chunk→page→batch upsert block
DO $$
DECLARE
  inner_limit  INT := {{var_page_size}};      -- page size
  inner_offset INT := 0;                      -- page size iterator
  outer_limit  INT := {{outer_limit}};        -- chunk size
  outer_offset INT := {{outer_offset}};       -- chunk offset provided by task expansion
  batch_limit  INT := {{var_batch_size}};     -- batch size

  -- in-memory storage for the deduped page
  page_rows     {{target_table}}_row[];        -- sales_order_item_flat_row
  total_rows    INT;                           -- count of page_rows
  start_idx     INT;
  end_idx       INT;
BEGIN
  WHILE inner_offset < outer_limit LOOP

    WITH
    -- 1) Page just the detail table by item_id
    raw_items AS (
      SELECT *
      FROM {{source_schema}}.{{source_table[0]}}  -- ods.salesorderitem
      ORDER BY item_id
      LIMIT inner_limit
      OFFSET inner_offset + outer_offset
    ),

    -- 2) Join in parent dimensions for only that page
    joined_page AS (
      SELECT
        ri.item_id,
        ri.order_id,
        so.order_number,
        so.created_at   AS order_created_at,
        so.order_total,
        so.total_qty_ordered,
        cust.id         AS customer_id,
        cust.first_name || ' ' || cust.last_name AS customer_name,
        cust.gender     AS customer_gender,
        cust.email      AS customer_email,
        prod.product_id,
        prod.product_sku,
        prod.product_name,
        ri.price        AS item_price,
        ri.qty_ordered  AS item_qty_order,
        ri.line_total   AS item_unit_total,
        ri.modified_at
      FROM raw_items ri
      JOIN {{source_schema}}.{{source_table[1]}} so   ON ri.order_id   = so.id     -- ods.salesorder
      JOIN {{source_schema}}.{{source_table[2]}} cust ON so.customer_id = cust.id      -- ods.customer
      JOIN {{source_schema}}.{{source_table[3]}} prod ON ri.product_id  = prod.product_id      -- ods.product
      WHERE ri.item_id      IS NOT NULL
        AND ri.order_id     IS NOT NULL
        AND so.order_number IS NOT NULL AND TRIM(so.order_number) <> ''
        AND so.created_at   IS NOT NULL
        AND so.order_total  IS NOT NULL AND so.order_total >= 0
        AND so.total_qty_ordered IS NOT NULL AND so.total_qty_ordered >= 0
        AND cust.id IS NOT NULL
        AND cust.first_name IS NOT NULL AND TRIM(cust.first_name) <> ''
        AND cust.last_name  IS NOT NULL AND TRIM(cust.last_name) <> ''
        AND (cust.gender IS NULL OR cust.gender IN ('Female','Male'))
        AND cust.email IS NOT NULL AND TRIM(cust.email)::VARCHAR(255) ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
        AND prod.product_id IS NOT NULL
        AND prod.product_sku IS NOT NULL AND TRIM(prod.product_sku) <> ''
        AND prod.product_name IS NOT NULL AND TRIM(prod.product_name) <> ''
        AND ri.price IS NOT NULL AND ri.price >= 0
        AND ri.qty_ordered IS NOT NULL AND ri.qty_ordered >= 0
        AND ri.line_total IS NOT NULL AND ri.line_total >= 0
    ),

    -- 3) Dedupe per (item_id,order_id), keep latest by modified_at
    deduped_page AS (
      SELECT
        item_id,
        order_id,
        (ARRAY_AGG(order_number       ORDER BY modified_at DESC))[1]::VARCHAR(50) AS order_number,
        (ARRAY_AGG(order_created_at   ORDER BY modified_at DESC))[1] AS order_created_at,
        MAX(modified_at)                                        AS modified_at,
        (ARRAY_AGG(order_total        ORDER BY modified_at DESC))[1]::DOUBLE PRECISION AS order_total,
        (ARRAY_AGG(total_qty_ordered  ORDER BY modified_at DESC))[1] AS total_qty_ordered,
        (ARRAY_AGG(customer_id        ORDER BY modified_at DESC))[1] AS customer_id,
        (ARRAY_AGG(customer_name      ORDER BY modified_at DESC))[1]::VARCHAR(200) AS customer_name,
        (ARRAY_AGG(customer_gender    ORDER BY modified_at DESC))[1]::VARCHAR(10) AS customer_gender,
        (ARRAY_AGG(customer_email     ORDER BY modified_at DESC))[1]::VARCHAR(255) AS customer_email,
        (ARRAY_AGG(product_id         ORDER BY modified_at DESC))[1] AS product_id,
        (ARRAY_AGG(product_sku        ORDER BY modified_at DESC))[1]::VARCHAR(50) AS product_sku,
        (ARRAY_AGG(product_name       ORDER BY modified_at DESC))[1]::VARCHAR(255) AS product_name,
        (ARRAY_AGG(item_price         ORDER BY modified_at DESC))[1]::DOUBLE PRECISION AS item_price,
        (ARRAY_AGG(item_qty_order     ORDER BY modified_at DESC))[1] AS item_qty_order,
        (ARRAY_AGG(item_unit_total    ORDER BY modified_at DESC))[1]::DOUBLE PRECISION AS item_unit_total
      FROM joined_page
      GROUP BY item_id, order_id
    )

    -- 4) Load deduped_page into a PL/pgSQL array
    SELECT ARRAY_AGG(
      ROW(
        item_id, order_id, order_number, order_created_at,
        order_total, total_qty_ordered, customer_id,
        customer_name, customer_gender, customer_email,
        product_id, product_sku, product_name,
        item_price, item_qty_order, item_unit_total
      )::{{target_table}}_row   -- sales_order_item_flat_row
    ) INTO page_rows
    FROM deduped_page;

    -- 5) Sub‐batch loop over the in‐memory array
    total_rows := COALESCE(array_length(page_rows,1), 0);
    start_idx  := 1;
    WHILE start_idx <= total_rows LOOP
      end_idx := LEAST(start_idx + batch_limit - 1, total_rows);

      INSERT INTO {{target_schema}}.{{target_table}} (      -- ods.sales_order_item_flat
        item_id, order_id, order_number, order_created_at,
        order_total, total_qty_ordered,
        customer_id, customer_name, customer_gender,
        customer_email, product_id, product_sku,
        product_name, item_price, item_qty_order,
        item_unit_total
      )
      SELECT
        (page_rows[i]).item_id,
        (page_rows[i]).order_id,
        (page_rows[i]).order_number,
        (page_rows[i]).order_created_at,
        (page_rows[i]).order_total,
        (page_rows[i]).total_qty_ordered,
        (page_rows[i]).customer_id,
        (page_rows[i]).customer_name,
        (page_rows[i]).customer_gender,
        (page_rows[i]).customer_email,
        (page_rows[i]).product_id,
        (page_rows[i]).product_sku,
        (page_rows[i]).product_name,
        (page_rows[i]).item_price,
        (page_rows[i]).item_qty_order,
        (page_rows[i]).item_unit_total
      FROM generate_series(start_idx, end_idx) AS i
      ON CONFLICT (item_id, order_id) DO UPDATE
        SET
          order_number      = EXCLUDED.order_number,
          order_created_at  = EXCLUDED.order_created_at,
          order_total       = EXCLUDED.order_total,
          total_qty_ordered = EXCLUDED.total_qty_ordered,
          customer_id       = EXCLUDED.customer_id,
          customer_name     = EXCLUDED.customer_name,
          customer_gender   = EXCLUDED.customer_gender,
          customer_email    = EXCLUDED.customer_email,
          product_id        = EXCLUDED.product_id,
          product_sku       = EXCLUDED.product_sku,
          product_name      = EXCLUDED.product_name,
          item_price        = EXCLUDED.item_price,
          item_qty_order    = EXCLUDED.item_qty_order,
          item_unit_total   = EXCLUDED.item_unit_total
      WHERE
        ({{target_schema}}.{{target_table}}.order_number,     -- ods.sales_order_item_flat.order_number
         {{target_schema}}.{{target_table}}.order_created_at,
         {{target_schema}}.{{target_table}}.order_total,
         {{target_schema}}.{{target_table}}.total_qty_ordered,
         {{target_schema}}.{{target_table}}.customer_id,
         {{target_schema}}.{{target_table}}.customer_name,
         {{target_schema}}.{{target_table}}.customer_gender,
         {{target_schema}}.{{target_table}}.customer_email,
         {{target_schema}}.{{target_table}}.product_id,
         {{target_schema}}.{{target_table}}.product_sku,
         {{target_schema}}.{{target_table}}.product_name,
         {{target_schema}}.{{target_table}}.item_price,
         {{target_schema}}.{{target_table}}.item_qty_order,
         {{target_schema}}.{{target_table}}.item_unit_total)
        IS DISTINCT FROM
        (EXCLUDED.order_number,
         EXCLUDED.order_created_at,
         EXCLUDED.order_total,
         EXCLUDED.total_qty_ordered,
         EXCLUDED.customer_id,
         EXCLUDED.customer_name,
         EXCLUDED.customer_gender,
         EXCLUDED.customer_email,
         EXCLUDED.product_id,
         EXCLUDED.product_sku,
         EXCLUDED.product_name,
         EXCLUDED.item_price,
         EXCLUDED.item_qty_order,
         EXCLUDED.item_unit_total)
      ;

      start_idx := end_idx + 1;
    END LOOP;

    inner_offset := inner_offset + inner_limit;
  END LOOP;
END
$$;
