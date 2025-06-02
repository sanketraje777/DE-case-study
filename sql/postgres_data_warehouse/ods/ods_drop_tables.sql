-- Drop tables if they exist, respecting dependency order
DROP TABLE IF EXISTS ods.salesorderitem CASCADE;
DROP TABLE IF EXISTS ods.salesorder CASCADE;
DROP TABLE IF EXISTS ods.customer CASCADE;
DROP TABLE IF EXISTS ods.product CASCADE;
