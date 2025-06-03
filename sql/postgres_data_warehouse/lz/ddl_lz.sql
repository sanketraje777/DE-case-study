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
  created_at TIMESTAMP WITH TIME ZONE,
  modified_at TIMESTAMP WITH TIME ZONE,
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
  created_at TIMESTAMP WITH TIME ZONE,
  modified_at TIMESTAMP WITH TIME ZONE
);
