SELECT * FROM lz.customer;
SELECT * FROM lz.salesorder;
SELECT * FROM lz.salesorderitem;

SELECT DISTINCT s.* FROM lz.salesorder s
JOIN lz.salesorderitem si
ON s.id = si.order_id

-- check indexes
SELECT indexname, indexdef
FROM pg_indexes
WHERE tablename = 'salesorder' AND schemaname = 'ods';

-- check constraints
SELECT conname, contype, conrelid::regclass AS table_name
FROM pg_constraint
WHERE conrelid = 'ods.salesorder'::regclass;

-- DESC table
SELECT
  column_name,
  data_type,
  is_nullable,
  column_default
FROM information_schema.columns
WHERE table_schema = 'pg_catalog'
AND table_name = 'pg_constraint';

SELECT drop_special_constraints

CREATE INDEX IF NOT EXISTS 
    idx_lz_salesorderitem_item_id ON lz.salesorderitem (item_id);
CREATE INDEX IF NOT EXISTS 
    idx_lz_salesorderitem_order_id ON lz.salesorderitem (order_id);
CREATE INDEX IF NOT EXISTS 
    idx_lz_salesorderitem_product_id ON lz.salesorderitem (product_id);

-- Drop constraints for salesorderitem
ALTER TABLE ods.salesorderitem 
    DROP CONSTRAINT IF EXISTS salesorderitem_pkey CASCADE,
    DROP CONSTRAINT IF EXISTS salesorderitem_order_id_fkey CASCADE,
    DROP CONSTRAINT IF EXISTS salesorderitem_product_id_fkey CASCADE,
    DROP CONSTRAINT IF EXISTS salesorderitem_qty_ordered_check CASCADE,
    DROP CONSTRAINT IF EXISTS salesorderitem_price_check CASCADE,
    DROP CONSTRAINT IF EXISTS salesorderitem_line_total_check CASCADE,
    DROP CONSTRAINT IF EXISTS salesorderitem_modified_at_check CASCADE;

DO $$
DECLARE
inner_limit  INT := %(var_batch_size)s;     -- rows per inner batch
inner_offset INT := 0;                      -- offset within this task’s outer slice
outer_limit  INT := %(outer_limit)s;        -- provided by task expansion
outer_offset INT := %(outer_offset)s;       -- provided by task expansion
BEGIN
WHILE inner_offset < outer_limit LOOP
	WITH deduped_products AS (
	SELECT
		product_id,
		ARRAY_AGG(TRIM(product_sku)::VARCHAR(50) ORDER BY modified_at DESC)[1] AS product_sku,
		ARRAY_AGG(TRIM(product_name)::VARCHAR(255) ORDER BY modified_at DESC)[1] AS product_name
	FROM (
		SELECT * FROM lz.salesorderitem
		WHERE product_id IS NOT NULL
		AND product_sku IS NOT NULL
		AND product_name IS NOT NULL
		ORDER BY product_id
		LIMIT inner_limit
		OFFSET inner_offset + outer_offset
	) sub
	GROUP BY product_id
	)
	INSERT INTO ods.product (product_id, product_sku, product_name)
	SELECT * FROM deduped_products
	ON CONFLICT ON CONSTRAINT product_pkey DO UPDATE
	SET product_sku  = EXCLUDED.product_sku,
		product_name = EXCLUDED.product_name
	WHERE ods.product.product_sku  IS DISTINCT FROM EXCLUDED.product_sku
		OR ods.product.product_name IS DISTINCT FROM EXCLUDED.product_name
	;
	inner_offset := inner_offset + inner_limit;
END LOOP;
END
$$;

SELECT * FROM lz.salesorder;

SELECT COUNT(*)
FROM lz.salesorder
WHERE id IS NOT NULL
AND customer_id IS NOT NULL
AND order_number IS NOT NULL
AND created_at IS NOT NULL
AND modified_at IS NOT NULL
AND modified_at >= created_at
AND order_total IS NOT NULL 
AND order_total >= 0
AND total_qty_ordered IS NOT NULL
AND total_qty_ordered >= 0;

CREATE SCHEMA IF NOT EXISTS lz;

DROP TABLE IF EXISTS lz.customer;
CREATE TABLE lz.customer (
  id INT,
  first_name VARCHAR(100),
  last_name VARCHAR(100),
  gender VARCHAR(10),
  email VARCHAR(255),
  billing_address TEXT,
  shipping_address TEXT
);

DROP TABLE IF EXISTS lz.salesorder;
CREATE TABLE lz.salesorder (
  id INT,
  customer_id INT,
  order_number VARCHAR(50),
  created_at TIMESTAMP,
  modified_at TIMESTAMP,
  order_total DECIMAL(10,2),
  total_qty_ordered INT
);

DROP TABLE IF EXISTS lz.salesorderitem;
CREATE TABLE lz.salesorderitem (
  item_id INT,
  order_id INT,
  product_id INT,
  product_sku VARCHAR(50),
  product_name VARCHAR(255),
  qty_ordered INT,
  price DECIMAL(10,2),
  line_total DECIMAL(12,2),
  created_at TIMESTAMP,
  modified_at TIMESTAMP
);

select * from lz.salesorder;
select * from lz.customer;
select * from lz.salesorderitem;

SELECT *
FROM lz.customer   -- lz.customer
WHERE id             IS NOT NULL
AND first_name       IS NOT NULL
AND last_name        IS NOT NULL
AND (gender IS NULL 
	OR LOWER(gender) IN ('female','male')) 
AND email            IS NOT NULL
AND shipping_address IS NOT NULL
AND email ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$';

select * from ods.salesorderitem;
select * from ods.salesorder;
select * from ods.customer;
select * from ods.product;
select * from ods.load_audit;

-- Run this **once** before you ever call the DO block:
DROP TYPE IF EXISTS salesorderitem_row;

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

DROP TABLE IF EXISTS public.load_audit CASCADE;

CREATE TABLE IF NOT EXISTS public.load_audit (
  table_name TEXT PRIMARY KEY,
  last_loaded TIMESTAMP WITH TIME ZONE NOT NULL,
  data_quality TEXT
);

SELECT order_number, COUNT(*) FROM ods.salesorder GROUP BY order_number HAVING COUNT(*) > 1;

SELECT * FROM public.load_audit;

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

DROP TABLE IF EXISTS dm.sales_order_item_flat;

CREATE TABLE dm.sales_order_item_flat (
  item_id INT NOT NULL,
  order_id INT NOT NULL,
  order_number VARCHAR(50) NOT NULL,
  order_created_at TIMESTAMP WITH TIME ZONE NOT NULL,
  order_total DOUBLE PRECISION NOT NULL CHECK (order_total >= 0),
  total_qty_ordered INT NOT NULL CHECK (total_qty_ordered >= 0),
  customer_id INT NOT NULL,
  customer_name VARCHAR(200) NOT NULL,
  customer_gender VARCHAR(10) CHECK (customer_gender IN ('Female', 'Male')),
  customer_email VARCHAR(255) NOT NULL,
  product_id INT NOT NULL,
  product_sku VARCHAR(50) NOT NULL,
  product_name VARCHAR(255) NOT NULL,
  item_price DOUBLE PRECISION NOT NULL CHECK (item_price >= 0),
  item_qty_order INT NOT NULL CHECK (item_qty_order >= 0),
  item_unit_total DOUBLE PRECISION NOT NULL CHECK (item_unit_total >= 0),
  PRIMARY KEY (item_id, order_id),
  CHECK (customer_email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
);

SHOW timezone;

-- SET TIME ZONE 'Asia/Dubai';

ALTER ROLE postgres SET TIME ZONE 'Asia/Dubai';

SHOW timezone;

select * from dm.sales_order_item_flat;
select * from public.load_audit;
