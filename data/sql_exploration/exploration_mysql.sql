-- Step 1: Pull only modified items (and include all fields if needed)
CREATE TEMPORARY TABLE temp_filtered_items AS
SELECT *
FROM salesorderitem
WHERE modified_at > '2021-07-06 00:00:00'
  AND modified_at <= '2021-07-07 23:59:59';

-- Step 2: Deduplicate order_ids
CREATE TEMPORARY TABLE temp_filtered_order_ids AS
SELECT DISTINCT order_id FROM temp_filtered_items;

-- Step 3: Get related sales orders
CREATE TEMPORARY TABLE temp_filtered_orders AS
SELECT s.id AS order_id, s.customer_id
FROM salesorder s
JOIN temp_filtered_order_ids fo ON s.id = fo.order_id;

-- Step 4: Get unique customers
CREATE TEMPORARY TABLE temp_filtered_customers AS
SELECT DISTINCT customer_id FROM temp_filtered_orders;

-- Step 5: Final full row fetches
SELECT * FROM temp_filtered_items;

SELECT s.* FROM salesorder s
JOIN temp_filtered_orders fo ON s.id = fo.order_id;

SELECT c.* FROM customer c
JOIN temp_filtered_customers fc ON c.id = fc.customer_id;

SELECT * FROM customer;

SELECT
  MIN(modified_at) AS min_modified_at,
  MAX(modified_at) AS max_modified_at,
  MIN(item_id) AS min_item_id,
  MAX(item_id) AS max_item_id,
  MIN(order_id) AS min_order_id,
  MAX(order_id) AS max_order_id
FROM salesorderitem;

SELECT modified_at, item_id, order_id
FROM salesorderitem
WHERE modified_at = "2021-07-06 15:49:53"
ORDER BY modified_at
LIMIT 10000;

SELECT s.modified_at, s.id as order_id
FROM salesorder s
#WHERE modified_at = "2021-07-06 15:49:53"
JOIN salesorderitem si ON s.id = si.order_id
AND si.modified_at = "2021-07-06 15:49:53"
ORDER BY s.id
LIMIT 10000;

SELECT MIN(c.id) AS min_val, MAX(c.id) AS max_val
FROM customer c
WHERE EXISTS
	(SELECT 1
	FROM
	(SELECT s.customer_id 
	FROM salesorder s
	WHERE EXISTS
		(SELECT 1
		FROM
			(SELECT order_id FROM salesorderitem WHERE modified_at = "2021-07-06 15:49:53" 
			UNION
			SELECT id as order_id FROM salesorder WHERE modified_at = "2021-07-06 15:49:53")
		AS ssi 
		WHERE ssi.order_id = s.id))
	AS sc
	WHERE sc.customer_id = c.id);

SELECT *
FROM customer c
WHERE EXISTS
	(SELECT 1
	FROM
	(SELECT s.customer_id 
	FROM salesorder s
	WHERE EXISTS
		(SELECT 1
		FROM
			(SELECT order_id FROM salesorderitem WHERE modified_at = "2021-07-06 15:49:53" 
			UNION
			SELECT id as order_id FROM salesorder WHERE modified_at = "2021-07-06 15:49:53")
		AS ssi 
		WHERE ssi.order_id = s.id))
	AS sc
	WHERE sc.customer_id = c.id);
#AND c.id >= 10 AND c.id < 50;

SELECT DISTINCT c.*
FROM customer c
JOIN salesorder s
ON s.customer_id = c.id
JOIN salesorderitem si
ON si.order_id = s.id
WHERE 
(si.modified_at = "2021-07-06 15:49:53"
OR s.modified_at = "2021-07-06 15:49:53");
#AND c.id >= 10 AND c.id < 50;

SELECT MIN(c.id) AS min_val, MAX(c.id) AS max_val
FROM customer c
JOIN salesorder s
ON s.customer_id = c.id
JOIN salesorderitem si
ON si.order_id = s.id
WHERE s.modified_at = "2021-07-06 15:49:53"
OR si.modified_at = "2021-07-06 15:49:53";

(
  SELECT c.*
  FROM customer c
  JOIN salesorder s ON s.customer_id = c.id
  WHERE s.modified_at = '2021-07-06 15:49:53'
)
UNION
(
  SELECT c.*
  FROM customer c
  JOIN salesorder s ON s.customer_id = c.id
  JOIN salesorderitem si ON si.order_id = s.id
  WHERE si.modified_at = '2021-07-06 15:49:53'
);

CREATE INDEX idx_salesorder_modified ON salesorder(modified_at);
CREATE INDEX idx_salesorderitem_modified ON salesorderitem(modified_at);

SELECT DISTINCT c.*
FROM salesorderitem si
JOIN salesorder s ON si.order_id = s.id
JOIN customer c ON s.customer_id = c.id
WHERE si.modified_at = '2021-07-06 15:49:53'
UNION
SELECT DISTINCT c.*
FROM salesorder s
JOIN customer c ON s.customer_id = c.id
WHERE s.modified_at = '2021-07-06 15:49:53';

CREATE TEMPORARY TABLE temp_customer AS
SELECT *
FROM customer c
WHERE EXISTS
	(SELECT 1
	FROM
	(SELECT s.customer_id 
	FROM salesorder s
	WHERE EXISTS
		(SELECT 1
		FROM
			(SELECT order_id FROM salesorderitem WHERE modified_at = "2021-07-06 15:49:53" 
			UNION
			SELECT id as order_id FROM salesorder WHERE modified_at = "2021-07-06 15:49:53")
		AS ssi 
		WHERE ssi.order_id = s.id))
	AS sc
	WHERE sc.customer_id = c.id);

ALTER TABLE temp_customer ADD PRIMARY KEY (id);
ALTER TABLE temp_customer ADD INDEX (id);

SELECT MAX(id), MIN(id) FROM temp_customer;
SELECT * FROM temp_customer WHERE id >= 10 AND id < 100;

DROP TEMPORARY TABLE IF EXISTS temp_customer;

SELECT *
FROM salesorderitem
WHERE line_total <> (qty_ordered * price);

SELECT soi.*, so.created_at AS salesorder_created_at, so.modified_at AS salesorder_modified_at
FROM salesorderitem soi
JOIN salesorder so ON soi.order_id = so.id
WHERE soi.modified_at <> so.modified_at
   OR soi.created_at <> so.created_at;

SELECT si.* 
FROM salesorderitem si
WHERE modified_at = "2021-07-06 15:49:53"
OR EXISTS
	(SELECT 1
    FROM salesorder s
    WHERE s.id = si.order_id)
LIMIT 10000;

SELECT si.*
FROM salesorderitem si
WHERE EXISTS
	(SELECT 1
	FROM
		(SELECT order_id FROM salesorderitem WHERE modified_at = "2021-07-06 15:49:53" 
		UNION
		SELECT id as order_id FROM salesorder WHERE modified_at = "2021-07-06 15:49:53")
	AS ssi 
	WHERE ssi.order_id = si.order_id);

SELECT s.*
FROM salesorder s
WHERE EXISTS
	(SELECT 1
	FROM
		(SELECT id AS order_id FROM salesorder WHERE modified_at = "2021-07-06 15:49:53" 
		UNION
		SELECT order_id FROM salesorderitem WHERE modified_at = "2021-07-06 15:49:53")
	AS ssi 
	WHERE ssi.order_id = s.id);

SELECT DISTINCT s.*
FROM salesorder s
JOIN salesorderitem si
ON si.order_id = s.id
WHERE si.modified_at = "2021-07-06 15:49:53"
OR s.modified_at = "2021-07-06 15:49:53";

SELECT DISTINCT si.*
FROM salesorderitem si
JOIN salesorder s
ON s.id = si.order_id
WHERE s.modified_at = "2021-07-06 15:49:53"
OR si.modified_at = "2021-07-06 15:49:53";

DROP TABLE IF EXISTS staging_customer;
CREATE TABLE staging_customer AS
SELECT *
FROM customer c
WHERE EXISTS
	(SELECT 1
	FROM
	(SELECT s.customer_id 
	FROM salesorder s
	WHERE EXISTS
		(SELECT 1
		FROM
			(SELECT order_id FROM salesorderitem WHERE modified_at = "2021-07-06 15:49:53" 
			UNION
			SELECT id as order_id FROM salesorder WHERE modified_at = "2021-07-06 15:49:53")
		AS ssi 
		WHERE ssi.order_id = s.id))
	AS sc
	WHERE sc.customer_id = c.id);
SELECT * FROM staging_customer;

SELECT MIN(c.id) AS min_val, MAX(c.id) AS max_val 
                FROM customer c 
                JOIN salesorder s 
                ON s.customer_id = c.id 
                JOIN salesorderitem si 
                ON si.order_id = s.id 
                WHERE (s.modified_at >= "2021-07-06 00:00:00" AND s.modified_at < "2021-07-07 00:00:00")
                OR (si.modified_at >= "2021-07-06 00:00:00" AND si.modified_at < "2021-07-07 00:00:00");

CREATE TABLE staging_customer AS
SELECT *
FROM customer c
WHERE EXISTS
	(SELECT 1
	FROM
	(SELECT s.customer_id 
	FROM salesorder s
	WHERE EXISTS
		(SELECT 1
		FROM
			(SELECT order_id FROM salesorderitem WHERE modified_at >= "2021-07-09 00:00:00" AND modified_at < "2021-07-10 00:00:00" 
			UNION
			SELECT id as order_id FROM salesorder WHERE modified_at >= "2021-07-09 00:00:00" AND modified_at < "2021-07-10 00:00:00")
		AS ssi 
		WHERE ssi.order_id = s.id))
	AS sc
	WHERE sc.customer_id = c.id);

SELECT order_id FROM salesorderitem WHERE modified_at >= "2021-07-09 00:00:00" AND modified_at < "2021-07-10 00:00:00";
SELECT id AS order_id FROM salesorder WHERE modified_at >= "2021-07-09 00:00:00" AND modified_at < "2021-07-10 00:00:00";

SELECT COUNT(*) AS count
FROM (
	SELECT DISTINCT c.*
	FROM customer c 
	JOIN salesorder s 
	ON s.customer_id = c.id 
	JOIN salesorderitem si 
	ON si.order_id = s.id 
	WHERE (s.modified_at >= "2021-07-06 00:00:00" AND s.modified_at < "2021-07-07 00:00:00")
	OR (si.modified_at >= "2021-07-06 00:00:00" AND si.modified_at < "2021-07-07 00:00:00")
) AS distinct_customers;

SELECT COUNT(*) AS count
FROM (
	SELECT DISTINCT si.* 
	FROM salesorderitem si 
	JOIN salesorder s 
	ON s.id = si.order_id 
	WHERE (s.modified_at >= "2021-07-06 00:00:00" AND s.modified_at < "2021-07-07 00:00:00")
	OR (si.modified_at >= "2021-07-06 00:00:00" AND si.modified_at < "2021-07-07 00:00:00")
) AS distinct_items;

DESC salesorder;

SELECT DISTINCT s.* 
FROM ounass_source.salesorder s 
JOIN ounass_source.salesorderitem si 
ON si.order_id = s.id 
WHERE (si.modified_at >= "2021-07-06 00:00:00" AND si.modified_at < "2021-07-07 00:00:00") 
OR (s.modified_at >= "2021-07-06 00:00:00" AND s.modified_at < "2021-07-07 00:00:00")
ORDER BY s.id
LIMIT 4804 OFFSET 0;

SELECT * from salesorder where id = 898;

select * from ounass_source.salesorder limit 1000;

SELECT DISTINCT si.* 
FROM salesorderitem si 
JOIN salesorder s 
ON s.id = si.order_id 
WHERE (s.modified_at >= "2021-07-06 00:00:00" AND s.modified_at < "2021-07-07 00:00:00")
OR (si.modified_at >= "2021-07-06 00:00:00" AND si.modified_at < "2021-07-07 00:00:00")
ORDER BY si.item_id, si.order_id
LIMIT 1000 OFFSET 0;
