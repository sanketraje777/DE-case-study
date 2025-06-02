-- Create source MySQL schema and tables
CREATE DATABASE IF NOT EXISTS ounass_source;
USE ounass_source;

-- Drop existing tables (in dependency order)
DROP TABLE IF EXISTS salesorderitem;
DROP TABLE IF EXISTS salesorder;
DROP TABLE IF EXISTS customer;

-- Customer Table
CREATE TABLE IF NOT EXISTS customer (
  id INT PRIMARY KEY,
  first_name VARCHAR(100) NOT NULL,
  last_name VARCHAR(100) NOT NULL,
  gender ENUM('Female', 'Male') NOT NULL,
  email VARCHAR(255) NOT NULL,
  billing_address TEXT,
  shipping_address TEXT NOT NULL,
  CHECK (email REGEXP '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$')
) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

-- Sales Order Table
CREATE TABLE IF NOT EXISTS salesorder (
  id INT PRIMARY KEY,
  customer_id INT NOT NULL,
  order_number VARCHAR(50) UNIQUE NOT NULL,
  created_at DATETIME NOT NULL,
  modified_at DATETIME NOT NULL,
  order_total DECIMAL(10, 2) NOT NULL,
  total_qty_ordered INT NOT NULL,
  CHECK (order_total >= 0),
  CHECK (total_qty_ordered >= 0),
  CHECK (modified_at >= created_at),
  FOREIGN KEY (customer_id) REFERENCES customer(id) ON DELETE CASCADE
) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

-- Sales Order Item Table
CREATE TABLE IF NOT EXISTS salesorderitem (
  item_id INT PRIMARY KEY,
  order_id INT NOT NULL,
  product_id INT NOT NULL,
  product_sku VARCHAR(50) NOT NULL,
  product_name VARCHAR(255),
  qty_ordered INT NOT NULL,
  price DECIMAL(10, 2) NOT NULL,
  line_total DECIMAL(12, 2) NOT NULL,
  created_at DATETIME NOT NULL,
  modified_at DATETIME NOT NULL,
  CHECK (qty_ordered >= 0),
  CHECK (price >= 0),
  CHECK (line_total >= 0),
  CHECK (modified_at >= created_at),  
  FOREIGN KEY (order_id) REFERENCES salesorder(id) ON DELETE CASCADE
) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
