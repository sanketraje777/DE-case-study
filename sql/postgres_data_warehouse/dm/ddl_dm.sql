-- Create destination PostgreSQL schema and table
CREATE SCHEMA IF NOT EXISTS dm;

DROP TABLE IF EXISTS dm.sales_order_item_flat;
CREATE TABLE dm.sales_order_item_flat (
  item_id INT NOT NULL,
  order_id INT NOT NULL,
  order_number VARCHAR(50) NOT NULL,
  order_created_at TIMESTAMP NOT NULL,
  order_total DOUBLE PRECISION NOT NULL,
  total_qty_ordered INT NOT NULL,
  customer_id INT NOT NULL,
  customer_name VARCHAR(200) NOT NULL,
  customer_gender VARCHAR(10) NOT NULL,
  customer_email VARCHAR(255) NOT NULL,
  product_id INT NOT NULL,
  product_sku VARCHAR(50) NOT NULL,
  product_name VARCHAR(255),
  item_price DOUBLE PRECISION NOT NULL,
  item_qty_order INT NOT NULL,
  item_unit_total DOUBLE PRECISION NOT NULL,
  PRIMARY KEY (order_id, item_id)
) PARTITION BY HASH (item_id, order_id);

DO $$
BEGIN
  FOR i IN 0..16 LOOP
    EXECUTE format($sql$
      CREATE TABLE IF NOT EXISTS dm.sales_order_item_flat_p%1$s
      PARTITION OF dm.sales_order_item_flat
      FOR VALUES WITH (MODULUS 16, REMAINDER %1$s);
    $sql$, i);
  END LOOP;
END;
$$;
