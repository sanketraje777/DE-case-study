-- Create the composite type (run once)
CREATE TYPE sales_order_item_flat_row AS (
  item_id            INT,
  order_id           INT,
  order_number       VARCHAR(50),
  order_created_at   TIMESTAMPTZ,
  order_total        DOUBLE PRECISION,
  total_qty_ordered  INT,
  customer_id        INT,
  customer_name      VARCHAR(200),
  customer_gender    VARCHAR(10),
  customer_email     VARCHAR(255),
  product_id         INT,
  product_sku        VARCHAR(50),
  product_name       VARCHAR(255),
  item_price         DOUBLE PRECISION,
  item_qty_order     INT,
  item_unit_total    DOUBLE PRECISION
);

-- To drop type sales_order_item_flat_row
-- DROP TYPE IF EXISTS sales_order_item_flat_row;
