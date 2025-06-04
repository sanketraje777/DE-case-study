-- The chunk→page→batch upsert block
DO $$
DECLARE
  inner_limit  INT := 10000;      -- page size
  inner_offset INT := 0;                      -- page size iterator
  outer_limit  INT := 7561;        -- chunk size
  outer_offset INT := 0;       -- chunk offset provided by task expansion
  batch_limit  INT := 1000;     -- batch size
  -- in-memory storage for the deduped page
  page_rows     sales_order_item_flat_row[];        -- sales_order_item_flat_row
  total_rows    INT;                           -- count of page_rows
  start_idx     INT;
  end_idx       INT;
BEGIN
  WHILE inner_offset < outer_limit LOOP
    WITH
    -- 1) Page just the detail table by item_id
    raw_items AS (
      SELECT *
      FROM ods.salesorderitem  -- ods.salesorderitem
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
      JOIN ods.salesorder so   ON ri.order_id   = so.id     -- ods.salesorder
      JOIN ods.customer cust ON so.customer_id = cust.id      -- ods.customer
      JOIN ods.product prod ON ri.product_id  = prod.product_id      -- ods.product
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
		order_number::VARCHAR(50) AS order_number,
        order_created_at AS order_created_at,
        modified_at AS modified_at,
		order_total::DOUBLE PRECISION AS order_total,
        total_qty_ordered AS total_qty_ordered,
        customer_id AS customer_id,
        customer_name::VARCHAR(200) AS customer_name,
        customer_gender::VARCHAR(10) AS customer_gender,
        customer_email::VARCHAR(255) AS customer_email,
        product_id AS product_id,
        product_sku::VARCHAR(50) AS product_sku,
        product_name::VARCHAR(255) AS product_name,
        item_price::DOUBLE PRECISION AS item_price,
        item_qty_order AS item_qty_order,
        item_unit_total::DOUBLE PRECISION AS item_unit_total,
        modified_at,
        ROW_NUMBER() OVER (
            PARTITION BY item_id, order_id
            ORDER BY modified_at DESC
        ) AS rn
      FROM joined_page
    ORDER BY item_id
    )

    -- 4) Load deduped_page into a PL/pgSQL array
    SELECT ARRAY_AGG(
      ROW(
        item_id, order_id, order_number, order_created_at,
        order_total, total_qty_ordered, customer_id,
        customer_name, customer_gender, customer_email,
        product_id, product_sku, product_name,
        item_price, item_qty_order, item_unit_total
      )::sales_order_item_flat_row   -- sales_order_item_flat_row
    ) INTO page_rows
    FROM deduped_page
	WHERE rn = 1;
    -- 5) Sub‐batch loop over the in‐memory array
    total_rows := COALESCE(array_length(page_rows,1), 0);
    start_idx  := 1;
    WHILE start_idx <= total_rows LOOP
      end_idx := LEAST(start_idx + batch_limit - 1, total_rows);
      INSERT INTO dm.sales_order_item_flat (      -- ods.sales_order_item_flat
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
      FROM generate_series(start_idx, end_idx) AS i;
      start_idx := end_idx + 1;
    END LOOP;
    inner_offset := inner_offset + inner_limit;
  END LOOP;
END
$$;

TRUNCATE TABLE dm.sales_order_item_flat;

INSERT INTO dm.sales_order_item_flat (
    item_id, order_id, order_number, order_created_at,
    order_total, total_qty_ordered,
    customer_id, customer_name, customer_gender,
    customer_email, product_id, product_sku,
    product_name, item_price, item_qty_order,
    item_unit_total
)
SELECT
    item_id,
    order_id,
    order_number,
    order_created_at,
    order_total,
    total_qty_ordered,
    customer_id,
    customer_name,
    customer_gender,
    customer_email,
    product_id,
    product_sku,
    product_name,
    item_price,
    item_qty_order,
    item_unit_total
FROM (
    SELECT
        ri.item_id,
        ri.order_id,
        so.order_number,
        so.created_at        AS order_created_at,
        so.order_total,
        so.total_qty_ordered,
        cust.id              AS customer_id,
        cust.first_name || ' ' || cust.last_name AS customer_name,
        cust.gender          AS customer_gender,
        cust.email           AS customer_email,
        prod.product_id,
        prod.product_sku,
        prod.product_name,
        ri.price             AS item_price,
        ri.qty_ordered       AS item_qty_order,
        ri.line_total        AS item_unit_total,
        ri.modified_at,
        ROW_NUMBER() OVER (
            PARTITION BY ri.item_id, ri.order_id
            ORDER BY ri.modified_at DESC
        ) AS rn
    FROM ods.salesorderitem ri
    JOIN ods.salesorder so   ON ri.order_id   = so.id
    JOIN ods.customer cust   ON so.customer_id = cust.id
    JOIN ods.product prod    ON ri.product_id  = prod.product_id
    WHERE ri.item_id IS NOT NULL
      AND ri.order_id IS NOT NULL
      AND so.order_number IS NOT NULL AND TRIM(so.order_number) <> ''
      AND so.created_at IS NOT NULL
      AND so.order_total IS NOT NULL AND so.order_total >= 0
      AND so.total_qty_ordered IS NOT NULL AND so.total_qty_ordered >= 0
      AND cust.id IS NOT NULL
      AND cust.first_name IS NOT NULL AND TRIM(cust.first_name) <> ''
      AND cust.last_name IS NOT NULL AND TRIM(cust.last_name) <> ''
      AND (cust.gender IS NULL OR cust.gender IN ('Female','Male'))
      AND cust.email IS NOT NULL AND TRIM(cust.email)::VARCHAR(255) ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
      AND prod.product_id IS NOT NULL
      AND prod.product_sku IS NOT NULL AND TRIM(prod.product_sku) <> ''
      AND prod.product_name IS NOT NULL AND TRIM(prod.product_name) <> ''
      AND ri.price IS NOT NULL AND ri.price >= 0
      AND ri.qty_ordered IS NOT NULL AND ri.qty_ordered >= 0
      AND ri.line_total IS NOT NULL AND ri.line_total >= 0
    ORDER BY ri.item_id
    LIMIT 7561  -- chunk size (outer_limit)
    OFFSET 0    -- chunk offset (outer_offset)
) AS deduped
WHERE deduped.rn = 1;  -- keep only the latest row for each (item_id, order_id)
