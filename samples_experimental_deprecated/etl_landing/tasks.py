from airflow.decorators import task
from airflow.providers.mysql.hooks.mysql import MySqlHook

@task()
def create_staging_table_customer(range_time: dict, table_name = "staging_customer", conn_id = "mysql_default"):
    mysql = MySqlHook(mysql_conn_id=conn_id)
    queries = [
        f"DROP TABLE IF EXISTS {table_name}",
        f"""
        CREATE TABLE `{table_name}` AS
        SELECT *
        FROM customer c
        WHERE EXISTS
            (SELECT 1
            FROM
            (SELECT s.customer_id 
            FROM salesorder s
            WHERE EXISTS
                (SELECT 1
                FROM
                    (SELECT order_id FROM salesorderitem WHERE modified_at >= %s AND modified_at < %s 
                    UNION
                    SELECT id AS order_id FROM salesorder WHERE modified_at >= %s AND modified_at < %s)
                AS ssi 
                WHERE ssi.order_id = s.id))
            AS sc
            WHERE sc.customer_id = c.id);
        """,
        "ALTER TABLE staging_customer ADD PRIMARY KEY (id);"
    ]
    """
    Can also use alternate query:
    SELECT DISTINCT c.*
    FROM customer c
    JOIN salesorder s
    ON s.customer_id = c.id
    JOIN salesorderitem si
    ON si.order_id = s.id
    WHERE 
    (si.modified_at >= %s AND si.modified_at < %s 
    OR s.modified_at >= %s AND s.modified_at < %s)
    """
    params = [None, (range_time["start_time"], range_time["end_time"], range_time["start_time"], range_time["end_time"]), None, None]
    for query, param in zip(queries, params):
        mysql.run(query, parameters=param)
    return f"Created staging table: {table_name} for range {range_time}"

"""
Some helpful queries:
    mysql_sql = f"DROP TABLE IF EXISTS `{table_name}`"
    postgres_sql = f"DROP TABLE IF EXISTS {table_name} CASCADE"
    mysql_sql = f"TRUNCATE TABLE `{table_name}`"
    postgres_sql = f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE"
"""
