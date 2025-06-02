DO $$
DECLARE
  clean_pattern TEXT := '([[:cntrl:]]|Â)+';  -- regex pattern for cleaning (not used below, but declared for consistency)
BEGIN

  WITH deduped_items AS NOT MATERIALIZED (
    SELECT DISTINCT ON (item_id)
      item_id,
      order_id,
      product_id,
      qty_ordered,
      price::NUMERIC(10,2)    AS price,
      line_total::NUMERIC(12,2) AS line_total,
      created_at,
      modified_at
    FROM lz.salesorderitem
    WHERE item_id IS NOT NULL
      AND order_id IS NOT NULL
      AND product_id IS NOT NULL
      AND qty_ordered IS NOT NULL       AND qty_ordered  >= 0
      AND price IS NOT NULL             AND price        >= 0
      AND line_total IS NOT NULL        AND line_total   >= 0
      AND created_at IS NOT NULL
      AND modified_at IS NOT NULL       AND modified_at >= created_at
    ORDER BY item_id, modified_at DESC
  )
  INSERT INTO ods.salesorderitem (
    item_id, order_id, product_id, qty_ordered,
    price, line_total, created_at, modified_at
  )
  SELECT
    item_id, order_id, product_id, qty_ordered,
    price, line_total, created_at, modified_at
  FROM deduped_items;

END $$;

TRUNCATE TABLE ods.salesorderitem CASCADE; 
SELECT drop_special_constraints_and_indexes('ods','salesorderitem',ARRAY['p','f','u']);
SELECT * FROM ods.salesorderitem;
SELECT * FROM lz.salesorderitem;

DELETE FROM ods.salesorderitem AS i
USING (
  SELECT i.item_id
  FROM ods.salesorderitem i
  LEFT JOIN ods.salesorder o ON i.order_id = o.id
  LEFT JOIN ods.product     p ON i.product_id = p.product_id
  WHERE o.id IS NULL OR p.product_id IS NULL
) AS orphan_ids
WHERE i.item_id = orphan_ids.item_id;

ALTER TABLE ods.salesorderitem 
    ADD CONSTRAINT salesorderitem_pkey PRIMARY KEY (item_id);
ALTER TABLE ods.salesorderitem 
    ADD CONSTRAINT salesorderitem_order_id_fkey FOREIGN KEY (order_id) REFERENCES ods.salesorder(id) 
        ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE ods.salesorderitem 
    ADD CONSTRAINT salesorderitem_product_id_fkey FOREIGN KEY (product_id) REFERENCES ods.product(product_id) 
        ON DELETE CASCADE ON UPDATE CASCADE;
CREATE INDEX IF NOT EXISTS 
    idx_salesorderitem_order_id ON ods.salesorderitem(order_id);
CREATE INDEX IF NOT EXISTS 
    idx_salesorderitem_product_id ON ods.salesorderitem(product_id);
CREATE INDEX IF NOT EXISTS
    idx_salesorderitem_item_id_order_id_modified_at ON ods.salesorderitem(item_id, order_id, modified_at DESC);
