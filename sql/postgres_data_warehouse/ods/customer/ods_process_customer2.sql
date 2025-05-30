DO $$
DECLARE
  inner_limit  INT := %(var_batch_size)s;     -- rows per inner batch
  inner_offset INT := 0;                      -- offset within this task’s outer slice
  outer_limit  INT := %(outer_limit)s;        -- provided by task expansion
  outer_offset INT := %(outer_offset)s;       -- provided by task expansion
BEGIN
  WHILE inner_offset < outer_limit LOOP

    WITH raw_page AS (
      SELECT *
      FROM lz.customer
      WHERE id               IS NOT NULL
        AND first_name       IS NOT NULL
        AND last_name        IS NOT NULL
        AND (gender IS NULL OR LOWER(gender) IN ('female','male'))
        AND email            IS NOT NULL
        AND shipping_address IS NOT NULL
        AND email ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
      ORDER BY id
      LIMIT inner_limit
      OFFSET inner_offset + outer_offset
    ),

    deduped_customers AS (
      SELECT
        id,
        MIN(TRIM(first_name))::VARCHAR(100)         AS first_name,
        MIN(TRIM(last_name))::VARCHAR(100)          AS last_name,
        MIN(INITCAP(TRIM(gender)))::VARCHAR(10)     AS gender,
        MIN(LOWER(TRIM(email)))::VARCHAR(255)       AS email,
        MIN(TRIM(billing_address))                  AS billing_address,
        MIN(TRIM(shipping_address))                 AS shipping_address
      FROM raw_page
      GROUP BY id
    )

    INSERT INTO ods.customer (
      id,
      first_name,
      last_name,
      gender,
      email,
      billing_address,
      shipping_address
    )
    SELECT
      id,
      first_name,
      last_name,
      gender,
      email,
      billing_address,
      shipping_address
    FROM deduped_customers
    ON CONFLICT ON CONSTRAINT customer_pkey DO UPDATE
      SET
        first_name       = EXCLUDED.first_name,
        last_name        = EXCLUDED.last_name,
        gender           = EXCLUDED.gender,
        email            = EXCLUDED.email,
        billing_address  = EXCLUDED.billing_address,
        shipping_address = EXCLUDED.shipping_address
    WHERE
      (ods.customer.first_name,
       ods.customer.last_name,
       ods.customer.gender,
       ods.customer.email,
       ods.customer.billing_address,
       ods.customer.shipping_address)
      IS DISTINCT FROM
      (EXCLUDED.first_name,
       EXCLUDED.last_name,
       EXCLUDED.gender,
       EXCLUDED.email,
       EXCLUDED.billing_address,
       EXCLUDED.shipping_address)
    ;

    inner_offset := inner_offset + inner_limit;
  END LOOP;
END
$$;
