-- Run this **once** before you ever call the ods_process_salesorder.sql
CREATE TYPE salesorder_row AS (
  id INT, 
  customer_id INT, 
  order_number VARCHAR(50),
  created_at TIMESTAMPTZ, 
  modified_at TIMESTAMPTZ,
  order_total NUMERIC(10, 2), 
  total_qty_ordered INT
);

-- To drop type salesorder_row
-- DROP TYPE IF EXISTS salesorder_row;
