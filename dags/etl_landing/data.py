# Data dictionary of SQL templates per table for etl_landing
sql = {
    "salesorderitem": {
        "prepare": """
            TRUNCATE TABLE {target_schema}.{target_table} RESTART IDENTITY CASCADE;
            SELECT drop_special_constraints_and_indexes('{target_schema}', '{target_table}', ARRAY[]::text[]);
        """,
        "count": """
            SELECT COUNT(*) AS count
            FROM (
                SELECT DISTINCT si.*
                FROM {source_schema}.{source_table} si
                JOIN salesorder s
                    ON s.id = si.order_id
                WHERE (s.modified_at >= %(start_time)s
                        AND s.modified_at < %(end_time)s)
                    OR (si.modified_at >= %(start_time)s
                        AND si.modified_at < %(end_time)s)
            ) AS distinct_items
        """,
        "process": """
            SELECT DISTINCT si.*
            FROM {source_schema}.{source_table} si
                JOIN salesorder s
                ON s.id = si.order_id
            WHERE (s.modified_at >= %(load_range_start_time)s
                    AND s.modified_at < %(load_range_end_time)s)
                OR (si.modified_at >= %(load_range_start_time)s
                    AND si.modified_at < %(load_range_end_time)s)
            ORDER BY si.item_id, si.order_id
            LIMIT %({source_table}_limit)s OFFSET %({source_table}_offset)s
        """,
        "index": """
            CREATE INDEX IF NOT EXISTS idx_{target_table}_item_id_modified_at
                ON {target_schema}.{target_table} (item_id, modified_at DESC);
            CREATE INDEX IF NOT EXISTS idx_{target_table}_product_id_modified_at
                ON {target_schema}.{target_table} (product_id, modified_at DESC);
            CREATE INDEX IF NOT EXISTS idx_{target_table}_product_sku_modified_at
                ON {target_schema}.{target_table} (product_sku, modified_at DESC);
        """,
        "mark_loaded": "/opt/airflow/sql/{target_sys_folder}/insert_load_audit.sql"
    },
    "salesorder": {
        "prepare": """
            TRUNCATE TABLE {target_schema}.{target_table} RESTART IDENTITY CASCADE;
            SELECT drop_special_constraints_and_indexes('{target_schema}', '{target_table}', ARRAY[]::text[]);
        """,
        "count": """
            SELECT COUNT(*) AS count
            FROM (
                SELECT DISTINCT s.*
                FROM {source_schema}.{source_table} s
                JOIN salesorderitem si
                    ON si.order_id = s.id
                WHERE (si.modified_at >= %(start_time)s
                        AND si.modified_at < %(end_time)s)
                    OR (s.modified_at >= %(start_time)s
                        AND s.modified_at < %(end_time)s)
            ) AS distinct_orders
        """,
        "process": """
            SELECT DISTINCT s.*
            FROM {source_schema}.{source_table} s
                JOIN salesorderitem si
                ON si.order_id = s.id
            WHERE (si.modified_at >= %(load_range_start_time)s
                    AND si.modified_at < %(load_range_end_time)s)
                OR (s.modified_at >= %(load_range_start_time)s
                    AND s.modified_at < %(load_range_end_time)s)
            ORDER BY s.id
            LIMIT %({source_table}_limit)s OFFSET %({source_table}_offset)s
        """,
        "index": """
            CREATE INDEX IF NOT EXISTS idx_{target_table}_id_modified_at
                ON {target_schema}.{target_table} (id, modified_at DESC);
            CREATE INDEX IF NOT EXISTS idx_{target_table}_order_number_modified_at
                ON {target_schema}.{target_table} (order_number, modified_at DESC);
        """,
        "mark_loaded": "/opt/airflow/sql/{target_sys_folder}/insert_load_audit.sql"
    },
    "customer": {
        "prepare": """
            TRUNCATE TABLE {target_schema}.{target_table} RESTART IDENTITY CASCADE;
            SELECT drop_special_constraints_and_indexes('{target_schema}', '{target_table}', ARRAY[]::text[]);
        """,
        "count": """
            SELECT COUNT(*) AS count
            FROM (
                SELECT DISTINCT c.*
                FROM {source_schema}.{source_table} c
                JOIN salesorder s
                    ON s.customer_id = c.id
                JOIN salesorderitem si
                    ON si.order_id = s.id
                WHERE (s.modified_at >= %(start_time)s
                        AND s.modified_at < %(end_time)s)
                    OR (si.modified_at >= %(start_time)s
                        AND si.modified_at < %(end_time)s)
            ) AS distinct_customers
        """,
        "process": """
            SELECT DISTINCT c.*
            FROM {source_schema}.{source_table} c
                JOIN salesorder s
                ON s.customer_id = c.id
                JOIN salesorderitem si
                ON si.order_id = s.id
            WHERE (s.modified_at >= %(load_range_start_time)s
                    AND s.modified_at < %(load_range_end_time)s)
                OR (si.modified_at >= %(load_range_start_time)s
                    AND si.modified_at < %(load_range_end_time)s)
            ORDER BY c.id
            LIMIT %({source_table}_limit)s OFFSET %({source_table}_offset)s
        """,
        "index": """
            CREATE INDEX IF NOT EXISTS idx_customer_id_first_name_last_name
                ON lz.customer (
                    id,
                    (
                        TRIM(REGEXP_REPLACE(first_name, '{text_clean_regex}', '', 'g')) || ' ' ||
                        TRIM(REGEXP_REPLACE(last_name, '{text_clean_regex}', '', 'g'))
                    )
                );
        """,
        "mark_loaded": "/opt/airflow/sql/{target_sys_folder}/insert_load_audit.sql"
    },
}
