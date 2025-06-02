-- Customer Table
CREATE TABLE ods.customer (
  id INT,
  first_name VARCHAR(100),
  last_name VARCHAR(100),
  gender VARCHAR(10),
  email VARCHAR(255),
  billing_address TEXT,
  shipping_address TEXT
);

-- Sales Order Table
CREATE TABLE ods.salesorder (
  id INT,
  customer_id INT,
  order_number VARCHAR(50),
  created_at TIMESTAMP WITH TIME ZONE,
  modified_at TIMESTAMP WITH TIME ZONE,
  order_total NUMERIC(10, 2),
  total_qty_ordered INT
);

-- Product Table
CREATE TABLE ods.product (
  product_id INT,
  product_sku VARCHAR(50),
  product_name VARCHAR(255)
);

-- Sales Order Item Table
CREATE TABLE ods.salesorderitem (
  item_id INT,
  order_id INT,
  product_id INT,
  qty_ordered INT,
  price NUMERIC(10, 2),
  line_total NUMERIC(12, 2),
  created_at TIMESTAMP WITH TIME ZONE,
  modified_at TIMESTAMP WITH TIME ZONE
);
