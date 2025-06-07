# Data dictionary of SQL templates for etl_ods
sql = {
    "salesorderitem": {
        "prepare": """
            TRUNCATE TABLE {target_schema}.{target_table} RESTART IDENTITY CASCADE;
            SELECT drop_special_constraints_and_indexes(
                '{target_schema}', '{target_table}', ARRAY['p','f','u']
            );
        """,
        "process": "/opt/airflow/sql/{target_sys_folder}/{target_schema}/"
                   "{target_table}/{target_schema}_process_{target_table}.sql",
        "process_fk": "/opt/airflow/sql/{target_sys_folder}/{target_schema}/"
                      "{target_table}/{target_schema}_process_fk_{target_table}.sql",
        "add_constraints": "/opt/airflow/sql/{target_sys_folder}/{target_schema}/"
                           "{target_table}/{target_schema}_create_{target_table}_constraints_indexes.sql",
        "dq_check": "/opt/airflow/sql/{target_sys_folder}/{target_schema}/"
                    "{target_table}/{target_schema}_{target_table}_data_quality_check.sql",
    },
    "salesorder": {
        "prepare": """
            TRUNCATE TABLE {target_schema}.{target_table} RESTART IDENTITY CASCADE;
            SELECT drop_special_constraints_and_indexes(
                '{target_schema}', '{target_table}', ARRAY['p','f','u']
            );
        """,
        "process": "/opt/airflow/sql/{target_sys_folder}/{target_schema}/"
                   "{target_table}/{target_schema}_process_{target_table}.sql",
        "process_fk": "/opt/airflow/sql/{target_sys_folder}/{target_schema}/"
                      "{target_table}/{target_schema}_process_fk_{target_table}.sql",
        "add_constraints": "/opt/airflow/sql/{target_sys_folder}/{target_schema}/"
                           "{target_table}/{target_schema}_create_{target_table}_constraints_indexes.sql",
        "dq_check": "/opt/airflow/sql/{target_sys_folder}/{target_schema}/"
                    "{target_table}/{target_schema}_{target_table}_data_quality_check.sql",
    },
    "customer": {
        "prepare": """
            TRUNCATE TABLE {target_schema}.{target_table} RESTART IDENTITY CASCADE;
            SELECT drop_special_constraints_and_indexes(
                '{target_schema}', '{target_table}', ARRAY['p','f','u']
            );
            ALTER TABLE {target_schema}.{target_table} 
                DROP CONSTRAINT IF EXISTS {target_table}_email_check CASCADE;
        """,
        "process": "/opt/airflow/sql/{target_sys_folder}/{target_schema}/"
                   "{target_table}/{target_schema}_process_{target_table}.sql",
        "process_fk": None,  # no foreign-key step for customer
        "add_constraints": "/opt/airflow/sql/{target_sys_folder}/{target_schema}/"
                           "{target_table}/{target_schema}_create_{target_table}_constraints_indexes.sql",
        "dq_check": "/opt/airflow/sql/{target_sys_folder}/{target_schema}/"
                    "{target_table}/{target_schema}_{target_table}_data_quality_check.sql",
    },
    "product": {
        "prepare": """
            TRUNCATE TABLE {target_schema}.{target_table} RESTART IDENTITY CASCADE;
            SELECT drop_special_constraints_and_indexes(
                '{target_schema}', '{target_table}', ARRAY['p','f','u']
            );
        """,
        "process": "/opt/airflow/sql/{target_sys_folder}/{target_schema}/"
                   "{target_table}/{target_schema}_process_{target_table}.sql",
        "process_fk": None,  # no foreign-key step for product
        "add_constraints": "/opt/airflow/sql/{target_sys_folder}/{target_schema}/"
                           "{target_table}/{target_schema}_create_{target_table}_constraints_indexes.sql",
        "dq_check": "/opt/airflow/sql/{target_sys_folder}/{target_schema}/"
                    "{target_table}/{target_schema}_{target_table}_data_quality_check.sql",
    },
}
