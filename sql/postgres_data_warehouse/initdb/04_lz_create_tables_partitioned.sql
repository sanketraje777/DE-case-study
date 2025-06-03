CREATE TABLE IF NOT EXISTS lz.customer (
  id INT,
  first_name VARCHAR(100),
  last_name VARCHAR(100),
  gender VARCHAR(10),
  email VARCHAR(255),
  billing_address TEXT,
  shipping_address TEXT
) PARTITION BY HASH (id);

-- create child partitions
DO $$
BEGIN
  FOR i IN 0..15 LOOP
    EXECUTE format($sql$
      CREATE TABLE IF NOT EXISTS lz.customer_p%1$s
      PARTITION OF lz.customer
      FOR VALUES WITH (MODULUS 16, REMAINDER %1$s);
    $sql$, i);
  END LOOP;
END;
$$;

CREATE TABLE IF NOT EXISTS lz.salesorder (
  id INT,
  customer_id INT,
  order_number VARCHAR(50),
  created_at TIMESTAMP WITH TIME ZONE,
  modified_at TIMESTAMP WITH TIME ZONE,
  order_total DECIMAL(10,2),
  total_qty_ordered INT
) PARTITION BY HASH (id);

-- create child partitions
DO $$
BEGIN
  FOR i IN 0..15 LOOP
    EXECUTE format($sql$
      CREATE TABLE IF NOT EXISTS lz.salesorder_p%1$s
      PARTITION OF lz.salesorder
      FOR VALUES WITH (MODULUS 16, REMAINDER %1$s);
    $sql$, i);
  END LOOP;
END;
$$;

CREATE TABLE IF NOT EXISTS lz.salesorderitem (
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
) PARTITION BY HASH (item_id);

-- create child partitions
DO $$
BEGIN
  FOR i IN 0..15 LOOP
    EXECUTE format($sql$
      CREATE TABLE IF NOT EXISTS lz.salesorderitem_p%1$s
      PARTITION OF lz.salesorderitem
      FOR VALUES WITH (MODULUS 16, REMAINDER %1$s);
    $sql$, i);
  END LOOP;
END;
$$;
