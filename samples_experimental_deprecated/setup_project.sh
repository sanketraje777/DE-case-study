#!/bin/bash

# Create Docker network
docker network create etl_network

# Start containers
docker-compose up -d

# Load MySQL source schema
echo "Setting up MySQL source schema using -u root"
mysql -h localhost -P 3307 -u root -p < sql/ddl_source.sql

# Load PostgreSQL schemas
echo "Setting up PostgreSQL schemas using -U postgres"
cat sql/ddl_dw.sql sql/ddl_ods.sql sql/ddl_lz.sql | \
psql -h localhost -U postgres -d data_warehouse

# Set Airflow connections (requires container to be running)
docker exec -it airflow airflow connections delete 'mysql_default'
docker exec -it airflow airflow connections add 'mysql_default' \
    --conn-type 'mysql' \
    --conn-login 'user' \
    --conn-password 'db123' \
    --conn-host 'db-mysql' \
    --conn-port '3306' \
    --conn-schema 'ounass_source' \
    --conn-extra '{"charset": "utf8mb4"}'

docker exec -it airflow airflow connections delete 'postgres_default'
docker exec -it airflow airflow connections add 'postgres_default' \
    --conn-type 'postgres' \
    --conn-login 'postgres' \
    --conn-password 'pg123' \
    --conn-host 'db-postgres' \
    --conn-port '5432' \
    --conn-schema 'data_warehouse'

# Copy and import Airflow variables
docker cp variables.json airflow:/opt/airflow/variables.json
docker exec -it airflow airflow variables import /opt/airflow/variables.json
