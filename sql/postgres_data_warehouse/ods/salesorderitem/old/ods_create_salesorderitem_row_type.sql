-- Run this **once** before you ever call the ods_process_salesorderitem.sql
CREATE TYPE salesorderitem_row AS (
  item_id     INT,
  order_id    INT,
  product_id  INT,
  qty_ordered INT,
  price       NUMERIC(10,2),
  line_total  NUMERIC(12,2),
  created_at  TIMESTAMPTZ,
  modified_at TIMESTAMPTZ
);

-- To drop type salesorderitem_row
-- DROP TYPE IF EXISTS salesorderitem_row;
