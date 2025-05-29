CREATE SCHEMA IF NOT EXISTS ods;

DROP TABLE IF EXISTS ods.customer;
CREATE TABLE ods.customer (
  id INT PRIMARY KEY,
  first_name VARCHAR(100),
  last_name VARCHAR(100),
  gender VARCHAR(10),
  email VARCHAR(255),
  billing_address TEXT,
  shipping_address TEXT
);

DROP TABLE IF EXISTS ods.salesorder;
CREATE TABLE ods.salesorder (
  id INT PRIMARY KEY,
  customer_id INT,
  order_number VARCHAR(50),
  created_at TIMESTAMP,
  modified_at TIMESTAMP,
  order_total DECIMAL(10,2),
  total_qty_ordered INT
);

DROP TABLE IF EXISTS ods.salesorderitem;
CREATE TABLE ods.salesorderitem (
  item_id INT PRIMARY KEY,
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
