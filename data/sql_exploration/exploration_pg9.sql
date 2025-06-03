DO $$
DECLARE
  clean_pattern TEXT := '([[:cntrl:]]|Â)+';  -- regex pattern to remove unwanted characters
BEGIN

  WITH cleaned_raw AS NOT MATERIALIZED (
    SELECT
      ri.item_id,
      ri.order_id,
      TRIM(REGEXP_REPLACE(so.order_number, clean_pattern, '', 'g')) AS order_number,
      so.created_at AS order_created_at,
      so.order_total,
      so.total_qty_ordered,
      cust.id AS customer_id,
      TRIM(
        REGEXP_REPLACE(cust.first_name || ' ' || cust.last_name, clean_pattern, '', 'g')
      ) AS customer_name,
      TRIM(REGEXP_REPLACE(cust.gender, clean_pattern, '', 'g')) AS customer_gender,
      TRIM(REGEXP_REPLACE(cust.email, clean_pattern, '', 'g')) AS customer_email,
      prod.product_id,
      TRIM(REGEXP_REPLACE(prod.product_sku,   clean_pattern, '', 'g')) AS product_sku,
      TRIM(REGEXP_REPLACE(prod.product_name,  clean_pattern, '', 'g')) AS product_name,
      ri.price AS item_price,
      ri.qty_ordered AS item_qty_order,
      ri.line_total AS item_unit_total,
      ri.modified_at
    FROM ods.salesorderitem AS ri        -- ods.salesorderitem
    JOIN ods.salesorder AS so        -- ods.salesorder
      ON ri.order_id = so.id
    JOIN ods.customer AS cust      -- ods.customer
      ON so.customer_id = cust.id
    JOIN ods.product AS prod      -- ods.product
      ON ri.product_id = prod.product_id
    WHERE ri.item_id               IS NOT NULL
      AND ri.order_id               IS NOT NULL
      AND so.order_number           IS NOT NULL
      AND so.created_at             IS NOT NULL
      AND so.order_total            IS NOT NULL
      AND so.order_total::DOUBLE PRECISION >= 0
      AND so.total_qty_ordered      IS NOT NULL
      AND so.total_qty_ordered     >= 0
      AND cust.id                   IS NOT NULL
      AND cust.first_name           IS NOT NULL
      AND cust.last_name            IS NOT NULL
      AND (cust.gender             IS NULL
           OR TRIM(REGEXP_REPLACE(cust.gender, clean_pattern, '', 'g')) IN ('Female','Male'))
      AND cust.email                IS NOT NULL
      AND prod.product_id           IS NOT NULL
      AND prod.product_sku          IS NOT NULL
      AND prod.product_name         IS NOT NULL
      AND ri.price                  IS NOT NULL
      AND ri.price::DOUBLE PRECISION >= 0
      AND ri.qty_ordered            IS NOT NULL
      AND ri.qty_ordered           >= 0
      AND ri.line_total             IS NOT NULL
      AND ri.line_total::DOUBLE PRECISION >= 0
      AND TRIM(REGEXP_REPLACE(so.order_number, clean_pattern, '', 'g')) <> ''
      AND TRIM(REGEXP_REPLACE(cust.first_name, clean_pattern, '', 'g')) <> ''
      AND TRIM(REGEXP_REPLACE(cust.last_name, clean_pattern, '', 'g')) <> ''
      AND TRIM(REGEXP_REPLACE(cust.email, clean_pattern, '', 'g'))::VARCHAR(255) 
         ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
      AND TRIM(REGEXP_REPLACE(prod.product_sku, clean_pattern, '', 'g')) <> ''
      AND TRIM(REGEXP_REPLACE(prod.product_name, clean_pattern, '', 'g')) <> ''
  ),
  deduped_items AS NOT MATERIALIZED (
    SELECT DISTINCT ON (item_id, order_id)
      item_id,
      order_id,
      order_number::VARCHAR(50),
      order_created_at,
      order_total::DOUBLE PRECISION,
      total_qty_ordered,
      customer_id,
      customer_name::VARCHAR(200),
      customer_gender::VARCHAR(10),
      customer_email::VARCHAR(255),
      product_id,
      product_sku::VARCHAR(50),
      product_name::VARCHAR(255),
      item_price::DOUBLE PRECISION,
      item_qty_order,
      item_unit_total::DOUBLE PRECISION
    FROM cleaned_raw
    ORDER BY
      item_id,
      order_id,
      modified_at DESC  -- keep latest modification per (item_id, order_id)
  )
  INSERT INTO dm.sales_order_item_flat (
    item_id, order_id, order_number, order_created_at,
    order_total, total_qty_ordered, customer_id,
    customer_name, customer_gender, customer_email,
    product_id, product_sku, product_name,
    item_price, item_qty_order, item_unit_total
  )
  SELECT
    item_id, order_id, order_number, order_created_at,
    order_total, total_qty_ordered, customer_id,
    customer_name, customer_gender, customer_email,
    product_id, product_sku, product_name,
    item_price, item_qty_order, item_unit_total
  FROM deduped_items;

END $$;

TRUNCATE TABLE dm.sales_order_item_flat CASCADE; 
SELECT drop_special_constraints_and_indexes('dm','sales_order_item_flat',ARRAY['p','f','u']);
SELECT * FROM dm.sales_order_item_flat;
SELECT * FROM ods.salesorderitem;
SELECT * FROM ods.salesorder;
SELECT * FROM ods.product;
SELECT * FROM ods.customer;

ALTER TABLE dm.sales_order_item_flat    -- dm.sales_order_item_flat 
    ADD CONSTRAINT sales_order_item_flat_pkey PRIMARY KEY (item_id, order_id);
ALTER TABLE dm.sales_order_item_flat
	ADD CONSTRAINT sales_order_item_flat_customer_email_check 
		CHECK (customer_email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$');
CREATE INDEX IF NOT EXISTS idx_sales_order_item_flat_order_number 
    ON dm.sales_order_item_flat (order_number);
CREATE INDEX IF NOT EXISTS idx_sales_order_item_flat_customer_id 
    ON dm.sales_order_item_flat (customer_id);
CREATE INDEX IF NOT EXISTS idx_sales_order_item_flat_product_id 
    ON dm.sales_order_item_flat (product_id);
CREATE INDEX IF NOT EXISTS idx_sales_order_item_flat_order_created_at 
    ON dm.sales_order_item_flat (order_created_at);
CREATE INDEX IF NOT EXISTS idx_sales_order_item_flat_product_sku 
    ON dm.sales_order_item_flat (product_sku);
