CREATE TABLE dm.sales_order_item_flat (
  item_id INT,
  order_id INT,
  order_number VARCHAR(50),
  order_created_at TIMESTAMP WITH TIME ZONE,
  order_total DOUBLE PRECISION,
  total_qty_ordered INT,
  customer_id INT,
  customer_name VARCHAR(200),
  customer_gender VARCHAR(10),
  customer_email VARCHAR(255),
  product_id INT,
  product_sku VARCHAR(50),
  product_name VARCHAR(255),
  item_price DOUBLE PRECISION,
  item_qty_order INT,
  item_unit_total DOUBLE PRECISION
);
