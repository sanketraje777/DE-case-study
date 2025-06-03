SELECT COUNT(*) AS count
FROM (
    SELECT DISTINCT si.* 
    FROM {{source.schema}}.{{source.tables[0]}} si      -- ounass_source.salesorderitem
    JOIN salesorder s 
    ON s.id = si.order_id 
    WHERE (s.modified_at >= %(start_time)s AND s.modified_at < %(end_time)s) 
    OR (si.modified_at >= %(start_time)s AND si.modified_at < %(end_time)s)
) AS distinct_items;
