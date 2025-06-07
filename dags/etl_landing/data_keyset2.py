# Data dictionary of SQL templates per table for etl_landing

from common.tasks import generate_num_chunks_task
from common.config import DAG_CONCURRENCY

sql = {
    "salesorderitem": {
        "truncate": """
            TRUNCATE TABLE {target_schema}.{target_table} RESTART IDENTITY CASCADE;
        """,
        "deindex": """
            SELECT drop_special_constraints_and_indexes('{target_schema}', '{target_table}', ARRAY[]::text[]);
        """,
        "chunk": {
            "handler": generate_num_chunks_task, 
            "kws": {
                "query": """
                    SELECT MIN(item_id) AS min_val, MAX(item_id) AS max_val
                    FROM (
                        SELECT DISTINCT si.item_id
                        FROM {source_schema}.{source_table} si
                            JOIN salesorder s
                            ON s.id = si.order_id
                        WHERE (s.modified_at >= %(start_time)s
                                AND s.modified_at < %(end_time)s)
                            OR (si.modified_at >= %(start_time)s
                                AND si.modified_at < %(end_time)s)
                    ) AS distinct_{source_table}
                    """, 
                "no_of_chunks": max(DAG_CONCURRENCY // 4, 1)
                },
            "kwd": "parameters"
        },
        "process": """
            SELECT DISTINCT si.*
            FROM {source_schema}.{source_table} si
                JOIN salesorder s
                ON s.id = si.order_id
            WHERE ((s.modified_at >= %(load_range_start_time)s
                    AND s.modified_at < %(load_range_end_time)s)
                OR (si.modified_at >= %(load_range_start_time)s
                    AND si.modified_at < %(load_range_end_time)s))
                AND si.item_id >= %({source_table}_start_val)s 
                    AND si.item_id < %({source_table}_end_val)s
        """,
        "index": """
            CREATE INDEX IF NOT EXISTS idx_{target_table}_item_id_modified_at
                ON {target_schema}.{target_table} (item_id, modified_at DESC);
            CREATE INDEX IF NOT EXISTS idx_{target_table}_product_id_modified_at_item_id
                ON {target_schema}.{target_table} (product_id, modified_at DESC, item_id);
            CREATE INDEX IF NOT EXISTS idx_{target_table}_product_sku_modified_at_item_id
                ON {target_schema}.{target_table} (product_sku, modified_at DESC, item_id);
        """,
        "mark_loaded": "/opt/airflow/sql/{target_sys_folder}/insert_load_audit.sql"
    },
    "salesorder": {
        "truncate": """
            TRUNCATE TABLE {target_schema}.{target_table} RESTART IDENTITY CASCADE;
        """,
        "deindex": """
            SELECT drop_special_constraints_and_indexes('{target_schema}', '{target_table}', ARRAY[]::text[]);
        """,
        "chunk": {
            "handler": generate_num_chunks_task, 
            "kws": {
                "query": """
                    SELECT MIN(id) AS min_val, MAX(id) AS max_val
                    FROM (
                        SELECT DISTINCT s.id
                        FROM {source_schema}.{source_table} s
                            JOIN salesorderitem si
                            ON si.order_id = s.id
                        WHERE (si.modified_at >= %(start_time)s
                                AND si.modified_at < %(end_time)s)
                            OR (s.modified_at >= %(start_time)s
                                AND s.modified_at < %(end_time)s)
                    ) AS distinct_{source_table}
                    """, 
                "no_of_chunks": max(DAG_CONCURRENCY // 4, 1)
                },
            "kwd": "parameters"
        },
        "process": """
            SELECT DISTINCT s.*
            FROM {source_schema}.{source_table} s
                JOIN salesorderitem si
                ON si.order_id = s.id
            WHERE ((si.modified_at >= %(load_range_start_time)s
                    AND si.modified_at < %(load_range_end_time)s)
                OR (s.modified_at >= %(load_range_start_time)s
                    AND s.modified_at < %(load_range_end_time)s))
                AND s.id >= %({source_table}_start_val)s 
                    AND s.id < %({source_table}_end_val)s                    
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
        "truncate": """
            TRUNCATE TABLE {target_schema}.{target_table} RESTART IDENTITY CASCADE;
        """,
        "deindex": """
            SELECT drop_special_constraints_and_indexes('{target_schema}', '{target_table}', ARRAY[]::text[]);
        """,
        "chunk": {
            "handler": generate_num_chunks_task, 
            "kws": {
                "query": """
                    SELECT MIN(id) AS min_val, MAX(id) AS max_val
                    FROM (
                        SELECT DISTINCT c.id
                        FROM {source_schema}.{source_table} c
                        JOIN salesorder s
                            ON s.customer_id = c.id
                        JOIN salesorderitem si
                            ON si.order_id = s.id
                        WHERE (s.modified_at >= %(start_time)s
                                AND s.modified_at < %(end_time)s)
                            OR (si.modified_at >= %(start_time)s
                                AND si.modified_at < %(end_time)s)
                    ) AS distinct_{source_table}
                    """, 
                "no_of_chunks": max(DAG_CONCURRENCY // 4, 1)
                },
            "kwd": "parameters"
        },
        "process": """
            SELECT DISTINCT c.*
            FROM {source_schema}.{source_table} c
                JOIN salesorder s
                ON s.customer_id = c.id
                JOIN salesorderitem si
                ON si.order_id = s.id
            WHERE ((s.modified_at >= %(load_range_start_time)s
                    AND s.modified_at < %(load_range_end_time)s)
                OR (si.modified_at >= %(load_range_start_time)s
                    AND si.modified_at < %(load_range_end_time)s))
                AND c.id >= %({source_table}_start_val)s 
                    AND c.id < %({source_table}_end_val)s
        """,
        "index": """
            CREATE INDEX IF NOT EXISTS idx_customer_id_first_name_last_name
                ON lz.customer (id, 
                        TRIM(REGEXP_REPLACE(first_name, '([[:cntrl:]]|Â| )+', '', 'g')), 
                        TRIM(REGEXP_REPLACE(last_name, '([[:cntrl:]]|Â| )+', '', 'g'))
                    );
        """,
        "mark_loaded": "/opt/airflow/sql/{target_sys_folder}/insert_load_audit.sql"
    },
}
