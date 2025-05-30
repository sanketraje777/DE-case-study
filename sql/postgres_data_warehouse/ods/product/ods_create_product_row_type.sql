-- Run this **once** before you ever call the ods_process_product.sql
CREATE TYPE product_row AS (
    product_id INT, 
    product_sku VARCHAR(50), 
    product_name VARCHAR(255)
);

-- To drop type product_row
-- DROP TYPE IF EXISTS product_row;
