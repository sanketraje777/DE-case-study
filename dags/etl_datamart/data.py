# ─── Data dictionary of SQL templates / file‐paths per target table ───────────────────────────────────
sql = {
    "sales_order_item_flat": {
        "prepare": """
            TRUNCATE TABLE {target_schema}.{target_table} RESTART IDENTITY CASCADE;
            SELECT drop_special_constraints_and_indexes(
                '{target_schema}', '{target_table}', ARRAY['p','f','u']
            );
            ALTER TABLE {target_schema}.{target_table}
                DROP CONSTRAINT IF EXISTS {target_table}_customer_email_check CASCADE;
        """,
        "process": "/opt/airflow/sql/{target_sys_folder}/{target_schema}/" \
            "{target_table}/{target_schema}_process_{target_table}.sql",
        "process_fk": None,  # no FK step for this table
        "add_constraints": "/opt/airflow/sql/{target_sys_folder}/{target_schema}/" \
            "{target_table}/{target_schema}_create_{target_table}_constraints_indexes.sql",
        "dq_check": "/opt/airflow/sql/{target_sys_folder}/{target_schema}/" \
            "{target_table}/{target_schema}_{target_table}_data_quality_check.sql",
        "mark_loaded": "/opt/airflow/sql/{target_sys_folder}/insert_load_audit.sql"
    }
    # Add additional tables here if needed:
    # "another_target_table": {
    #     "prepare": "...",
    #     "process": "...",
    #     "process_fk": "...",
    #     "add_constraints": "...",
    #     "dq_check": "...",
    #     "mark_loaded": "...",
    # }
}
