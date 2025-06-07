DO $$
DECLARE
  inner_limit  INT := %(var_batch_size)s;     -- rows per inner batch
  inner_offset INT := 0;                      -- offset within this task’s outer slice
  outer_limit  INT := %(outer_limit)s;        -- provided by task expansion
  outer_offset INT := %(outer_offset)s;       -- provided by task expansion
BEGIN
  WHILE inner_offset < outer_limit LOOP

    WITH raw_page AS (
    SELECT
        product_id,
        product_sku,
        product_name,
        modified_at
    FROM lz.salesorderitem
    WHERE product_id   IS NOT NULL
        AND product_sku  IS NOT NULL
        AND product_name IS NOT NULL
    ORDER BY product_id, modified_at DESC
    LIMIT inner_limit
    OFFSET inner_offset + outer_offset
    ),

    id_ranked AS (
    SELECT
        product_id,
        TRIM(product_sku)::VARCHAR(50) AS product_sku,
        TRIM(product_name)::VARCHAR(255) AS product_name,
        modified_at,
        ROW_NUMBER() OVER (
        PARTITION BY product_id
        ORDER BY modified_at DESC
        ) AS rn_id
    FROM raw_page
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

    deduped AS (
    SELECT product_id, product_sku, product_name
    FROM sku_ranked
    WHERE rn_sku = 1
    )

    INSERT INTO ods.product (product_id, product_sku, product_name)
    SELECT product_id, product_sku, product_name
    FROM deduped
    ON CONFLICT ON CONSTRAINT product_pkey DO UPDATE
      SET
        product_sku  = EXCLUDED.product_sku,
        product_name = EXCLUDED.product_name
    WHERE
      (ods.product.product_sku, ods.product.product_name)
      IS DISTINCT FROM
      (EXCLUDED.product_sku, EXCLUDED.product_name)
    ;

    inner_offset := inner_offset + inner_limit;
  END LOOP;
END
$$;
