DO $$
DECLARE
  clean_pattern TEXT := '([[:cntrl:]]|Â)+';  -- pattern for cleaning unwanted characters
BEGIN

WITH latest_per_id AS NOT MATERIALIZED (
  SELECT DISTINCT ON (id)
    id,
    customer_id,
    TRIM(REGEXP_REPLACE(order_number, clean_pattern, '', 'g'))::VARCHAR(50) AS order_number,
    created_at,
    modified_at,
    order_total::NUMERIC(10, 2),
    total_qty_ordered
  FROM lz.salesorder
  WHERE id IS NOT NULL
    AND customer_id IS NOT NULL
    AND order_number IS NOT NULL
    AND created_at IS NOT NULL
    AND modified_at IS NOT NULL AND modified_at >= created_at
    AND order_total IS NOT NULL AND order_total >= 0
    AND total_qty_ordered IS NOT NULL AND total_qty_ordered >= 0
    AND TRIM(REGEXP_REPLACE(order_number, clean_pattern, '', 'g')) <> ''
  ORDER BY id, modified_at DESC
),
latest_per_order_number AS NOT MATERIALIZED (
  SELECT DISTINCT ON (order_number)
    id,
    customer_id,
    order_number,
    created_at,
    modified_at,
    order_total,
    total_qty_ordered
  FROM latest_per_id
  ORDER BY order_number, modified_at DESC
)
INSERT INTO ods.salesorder (
  id, customer_id, order_number,
  created_at, modified_at,
  order_total, total_qty_ordered
)
SELECT
  id,
  customer_id,
  order_number,
  created_at,
  modified_at,
  order_total,
  total_qty_ordered
FROM latest_per_order_number;

END $$;

TRUNCATE TABLE ods.salesorder CASCADE; 
SELECT drop_special_constraints_and_indexes('ods','salesorderitem',ARRAY['p','f','u']);
SELECT drop_special_constraints_and_indexes('ods','salesorder',ARRAY['p','f','u']);
SELECT * FROM ods.salesorder;
SELECT * FROM lz.salesorder;

DELETE FROM ods.salesorder AS s
USING (
  SELECT s.id
  FROM ods.salesorder s
  LEFT JOIN ods.customer c ON s.customer_id = c.id
  WHERE c.id IS NULL
) AS orphan_ids
WHERE s.id = orphan_ids.id;

ALTER TABLE ods.salesorder 
    ADD CONSTRAINT salesorder_pkey PRIMARY KEY (id);
ALTER TABLE ods.salesorder 
    ADD CONSTRAINT salesorder_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES ods.customer(id) 
        ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE ods.salesorder 
    ADD CONSTRAINT salesorder_order_number_key UNIQUE (order_number);
-- Create indexes for salesorder
CREATE INDEX IF NOT EXISTS 
    idx_salesorder_customer_id ON ods.salesorder(customer_id);
CREATE INDEX IF NOT EXISTS
    idx_salesorder_modified_at ON ods.salesorder(modified_at);
