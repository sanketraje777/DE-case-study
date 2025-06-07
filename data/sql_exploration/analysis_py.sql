SELECT * FROM dm.sales_order_item_flat;
SELECT MIN(order_created_at) FROM dm.sales_order_item_flat;

SELECT order_created_at AT TIME ZONE 'UTC'
FROM dm.sales_order_item_flat
LIMIT 3;

SELECT COUNT(DISTINCT product_id)
FROM dm.sales_order_item_flat;

SELECT * FROM analysis.monthly_sales_summary
ORDER BY total_sales DESC;

SELECT
  TO_CHAR(order_created_at, 'Day') AS day_of_week,
  SUM(item_qty_order)
FROM dm.sales_order_item_flat
GROUP BY day_of_week
ORDER BY day_of_week;
