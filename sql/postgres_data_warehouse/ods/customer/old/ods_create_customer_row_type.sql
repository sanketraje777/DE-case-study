-- Run this **once** before you ever call the ods_process_customer.sql
CREATE TYPE customer_row AS (
  id INT,
  first_name VARCHAR(100),
  last_name VARCHAR(100),
  gender VARCHAR(10),
  email VARCHAR(255),
  billing_address TEXT,
  shipping_address TEXT
);

-- To drop type customer_row
-- DROP TYPE IF EXISTS customer_row;
