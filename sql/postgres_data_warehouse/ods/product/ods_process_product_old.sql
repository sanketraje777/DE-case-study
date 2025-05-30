DO $$
DECLARE
inner_limit  INT := %(var_batch_size)s;     -- rows per inner batch
inner_offset INT := 0;                      -- offset within this task’s outer slice
outer_limit  INT := %(outer_limit)s;        -- provided by task expansion
outer_offset INT := %(outer_offset)s;       -- provided by task expansion
BEGIN
WHILE inner_offset < outer_limit LOOP

    WITH deduped_products AS (
    SELECT
        product_id,
        ARRAY_AGG(TRIM(product_sku)::VARCHAR(50) ORDER BY modified_at DESC)[1] AS product_sku,
        ARRAY_AGG(TRIM(product_name)::VARCHAR(255) ORDER BY modified_at DESC)[1] AS product_name
    FROM (
        SELECT * FROM lz.salesorderitem
        WHERE product_id IS NOT NULL
        AND product_sku IS NOT NULL
        AND product_name IS NOT NULL
        ORDER BY product_id
        LIMIT inner_limit
        OFFSET inner_offset + outer_offset
    ) sub
    GROUP BY product_id
    )

    INSERT INTO ods.product (product_id, product_sku, product_name)
    SELECT * FROM deduped_products
    ON CONFLICT ON CONSTRAINT product_pkey DO UPDATE
    SET product_sku  = EXCLUDED.product_sku,
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
