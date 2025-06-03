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
  customer_email VARCHAR(255) NOT NULL 
    CHECK (customer_email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'),
  product_id INT NOT NULL,
  product_sku VARCHAR(50) NOT NULL,
  product_name VARCHAR(255) NOT NULL,
  item_price DOUBLE PRECISION NOT NULL CHECK (item_price >= 0),
  item_qty_order INT NOT NULL CHECK (item_qty_order >= 0),
  item_unit_total DOUBLE PRECISION NOT NULL CHECK (item_unit_total >= 0),
  PRIMARY KEY (item_id, order_id)
);
