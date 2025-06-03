-- Create source MySQL schema and tables
CREATE DATABASE IF NOT EXISTS ounass_source;
USE ounass_source;

DROP TABLE IF EXISTS salesorderitem;
DROP TABLE IF EXISTS salesorder;
DROP TABLE IF EXISTS customer;

CREATE TABLE customer (
  id INT PRIMARY KEY,
  first_name VARCHAR(100) NOT NULL,
  last_name VARCHAR(100) NOT NULL,
  gender ENUM('Female','Male') NOT NULL,
  email VARCHAR(255) NOT NULL,
  billing_address TEXT,
  shipping_address TEXT NOT NULL
);

CREATE TABLE salesorder (
  id INT PRIMARY KEY,
  customer_id INT NOT NULL,
  order_number VARCHAR(50) UNIQUE NOT NULL,
  created_at DATETIME NOT NULL,
  modified_at DATETIME NOT NULL,
  order_total DECIMAL(10,2) NOT NULL,
  total_qty_ordered INT NOT NULL,
  FOREIGN KEY (customer_id) REFERENCES customer(id)
);

CREATE TABLE salesorderitem (
  item_id INT PRIMARY KEY,
  order_id INT NOT NULL,
  product_id INT NOT NULL,
  product_sku VARCHAR(50) NOT NULL,
  product_name VARCHAR(255),
  qty_ordered INT NOT NULL,
  price DECIMAL(10,2) NOT NULL,
  line_total DECIMAL(12,2) NOT NULL,
  created_at DATETIME NOT NULL,
  modified_at DATETIME NOT NULL,
  FOREIGN KEY (order_id) REFERENCES salesorder(id)
);
