DO $$
DECLARE
  inner_limit  INT := {{var_page_size}};      -- page size
  inner_offset INT := 0;                      -- page size iterator
  outer_limit  INT := {{outer_limit}};        -- chunk size
  outer_offset INT := {{outer_offset}};       -- chunk offset provided by task expansion
  batch_limit  INT := {{var_batch_size}};     -- batch size

  -- in-memory storage for the deduped page
  page_rows    {{target_table}}_row[];  -- product_row
  total_rows   INT;
  start_idx    INT;
  end_idx      INT;
BEGIN
  WHILE inner_offset < outer_limit LOOP

    WITH raw_page AS (
    SELECT
        product_id,
        product_sku,
        product_name,
        modified_at
    FROM {{source_schema}}.{{source_table}}   -- lz.salesorderitem
    ORDER BY product_id, modified_at DESC
    LIMIT inner_limit
    OFFSET inner_offset + outer_offset
    ),

    cleaned_raw_page AS (
      SELECT 
        product_id,
        TRIM(product_sku),
        TRIM(product_name)
      FROM raw_page
      WHERE product_id   IS NOT NULL
        AND product_sku  IS NOT NULL AND TRIM(product_sku) <> ''
        AND product_name IS NOT NULL AND TRIM(product_name) <> ''
    ),

    id_ranked AS (
    SELECT
        product_id,
        product_sku,
        product_name,
        modified_at,
        ROW_NUMBER() OVER (
        PARTITION BY product_id
        ORDER BY modified_at DESC
        ) AS rn_id
    FROM cleaned_raw_page
    ),

    sku_ranked AS (
    SELECT
        product_id,
        product_sku,
        product_name,
        ROW_NUMBER() OVER (
        PARTITION BY product_sku
        ORDER BY modified_at DESC
        ) AS rn_sku
    FROM id_ranked
    WHERE rn_id = 1
    ),

    deduped_product AS (
    SELECT 
      product_id, 
      product_sku::VARCHAR(50), 
      product_name::VARCHAR(255)
    FROM sku_ranked
    WHERE rn_sku = 1
    )

    SELECT ARRAY_AGG(
	    ROW(
        product_id, product_sku, product_name
	    )::{{target_table}}_row   -- product_row	      
    )
      INTO page_rows
    FROM deduped_product d;

    -- 2) set up array‐bounds for sub‐batches
    total_rows := COALESCE(array_length(page_rows,1), 0);
    start_idx  := 1;

    -- 3) process sub‐batches in memory
    WHILE start_idx <= total_rows LOOP
      end_idx := LEAST(start_idx + batch_limit - 1, total_rows);

      INSERT INTO {{target_schema}}.{{target_table}} (
        product_id, product_sku, product_name)  -- ods.product
      SELECT 
        (page_rows[i]).product_id, 
        (page_rows[i]).product_sku, 
        (page_rows[i]).product_name
      FROM generate_series(start_idx, end_idx) AS i
      ON CONFLICT (product_id) DO UPDATE
        SET
          product_sku  = EXCLUDED.product_sku,
          product_name = EXCLUDED.product_name
      WHERE
        ({{target_schema}}.{{target_table}}.product_sku,    -- ods.product.product_sku
        {{target_schema}}.{{target_table}}.product_name)
        IS DISTINCT FROM
        (EXCLUDED.product_sku, EXCLUDED.product_name)
      ;

      start_idx := end_idx + 1;
    END LOOP;

    inner_offset := inner_offset + inner_limit;
  END LOOP;
END
$$;
