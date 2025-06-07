#!/bin/bash

airflow db migrate || airflow db init
airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com
airflow pools set db_pool 4 'Limit DB-intensive tasks'
airflow pools set db_pool_max 16 'Max Limit DB-intensive tasks'
airflow connections delete 'mysql_default'
airflow connections add 'mysql_default' \
    --conn-type 'mysql' \
    --conn-login 'user' \
    --conn-password 'db123' \
    --conn-host 'db-mysql' \
    --conn-port '3306' \
    --conn-schema 'ounass_source' \
    --conn-extra '{"charset": "utf8mb4"}'
airflow connections delete 'postgres_default'
airflow connections add 'postgres_default' \
    --conn-type 'postgres' \
    --conn-login 'postgres' \
    --conn-password 'pg123' \
    --conn-host 'db-postgres' \
    --conn-port '5432' \
    --conn-schema 'data_warehouse'
airflow variables import /opt/airflow/variables.json
