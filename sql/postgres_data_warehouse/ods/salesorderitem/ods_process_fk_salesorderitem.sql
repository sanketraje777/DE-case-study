DELETE FROM {{target_schema}}.{{target_table}} AS i
USING (
  SELECT i.item_id
  FROM {{target_schema}}.{{target_table}} i
  LEFT JOIN ods.salesorder o ON i.order_id = o.id
  LEFT JOIN ods.product p ON i.product_id = p.product_id
  WHERE o.id IS NULL OR p.product_id IS NULL
) AS orphan_ids
WHERE i.item_id = orphan_ids.item_id;
