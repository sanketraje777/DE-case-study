DO $$
DECLARE
  -- paging and batching parameters
  inner_limit  INT := {{var_page_size}};      -- page size
  inner_offset INT := 0;                      -- page size iterator
  outer_limit  INT := {{outer_limit}};        -- chunk size
  outer_offset INT := {{outer_offset}};       -- chunk offset provided by task expansion
  batch_limit  INT := {{var_batch_size}};     -- batch size

  -- in-memory storage for the deduped page
  page_rows    {{target_table}}_row[];  -- customer_row
  total_rows   INT;
  start_idx    INT;
  end_idx      INT;
BEGIN
  WHILE inner_offset < outer_limit LOOP

    WITH raw_page AS (
      SELECT *
      FROM {{source_schema}}.{{source_table}}   -- lz.customer
      ORDER BY id
      LIMIT inner_limit
      OFFSET inner_offset + outer_offset
    ),

    cleaned_raw_page AS (
      SELECT 
        id,
        TRIM(first_name) AS first_name,
        TRIM(last_name) AS last_name,
        INITCAP(TRIM(gender)) AS gender
        LOWER(TRIM(email)) AS email
        TRIM(billing_address) AS billing_address
        TRIM(shipping_address) AS shipping_address
      FROM raw_page
      WHERE id               IS NOT NULL
        AND first_name       IS NOT NULL AND TRIM(first_name)::VARCHAR(100) <> ''
        AND last_name        IS NOT NULL AND TRIM(last_name)::VARCHAR(100) <> ''
        AND (gender IS NULL OR LOWER(TRIM(gender)) IN ('female','male'))
        AND email            IS NOT NULL
        AND shipping_address IS NOT NULL AND TRIM(shipping_address) <> ''
        AND TRIM(email)::VARCHAR(255) ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
    ),

    deduped_customers AS (
      SELECT
        id,
        MIN(first_name)::VARCHAR(100)     AS first_name,
        MIN(last_name)::VARCHAR(100)      AS last_name,
        MIN(gender)::VARCHAR(10)          AS gender,
        MIN(email)::VARCHAR(255)          AS email,
        MIN(billing_address)              AS billing_address,
        MIN(shipping_address)             AS shipping_address
      FROM cleaned_raw_page
      GROUP BY id
    )

    SELECT ARRAY_AGG(
	    ROW(
        id, first_name, last_name,
        gender, email, 
        billing_address, shipping_address
	    )::{{target_table}}_row   -- customer_row	      
    )
      INTO page_rows
    FROM deduped_customers d;

    -- 2) set up array‐bounds for sub‐batches
    total_rows := COALESCE(array_length(page_rows,1), 0);
    start_idx  := 1;

    -- 3) process sub‐batches in memory
    WHILE start_idx <= total_rows LOOP
      end_idx := LEAST(start_idx + batch_limit - 1, total_rows);

      INSERT INTO {{target_schema}}.{{target_table}} (    -- ods.customer
        id, first_name, last_name,
        gender, email, 
        billing_address, shipping_address
      )
      SELECT
        (page_rows[i]).id,
        (page_rows[i]).first_name,
        (page_rows[i]).last_name,
        (page_rows[i]).gender,
        (page_rows[i]).email,
        (page_rows[i]).billing_address,
        (page_rows[i]).shipping_address
      FROM generate_series(start_idx, end_idx) AS i
      ON CONFLICT (id) DO UPDATE
        SET
          first_name       = EXCLUDED.first_name,
          last_name        = EXCLUDED.last_name,
          gender           = EXCLUDED.gender,
          email            = EXCLUDED.email,
          billing_address  = EXCLUDED.billing_address,
          shipping_address = EXCLUDED.shipping_address
      WHERE
        ({{target_schema}}.{{target_table}}.first_name,   -- ods.customer.first_name
        {{target_schema}}.{{target_table}}.last_name,
        {{target_schema}}.{{target_table}}.gender,
        {{target_schema}}.{{target_table}}.email,
        {{target_schema}}.{{target_table}}.billing_address,
        {{target_schema}}.{{target_table}}.shipping_address)
        IS DISTINCT FROM
        (EXCLUDED.first_name,
        EXCLUDED.last_name,
        EXCLUDED.gender,
        EXCLUDED.email,
        EXCLUDED.billing_address,
        EXCLUDED.shipping_address)
      ;

      start_idx := end_idx + 1;
    END LOOP;

    inner_offset := inner_offset + inner_limit;
  END LOOP;
END
$$;
