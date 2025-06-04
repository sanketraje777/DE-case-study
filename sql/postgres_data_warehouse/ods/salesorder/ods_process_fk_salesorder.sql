DELETE FROM {{target_schema}}.{{target_table}} AS s
USING (
  SELECT s.id
  FROM {{target_schema}}.{{target_table}} s
  LEFT JOIN ods.customer c ON s.customer_id = c.id
  WHERE c.id IS NULL
) AS orphan_ids
WHERE s.id = orphan_ids.id;
