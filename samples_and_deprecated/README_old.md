# Ounass Data Engineering Case Study

## Setup
1. Create Docker network: `docker network create mysql_default`
2. `docker-compose up -d`
3. Load source SQL: `mysql -h localhost -P 3307 -u root -p < sql/ddl_source.sql`
or for powershell: `Get-Content sql\ddl_source.sql | mysql -h localhost -P 3307 -u root -p`
4. Load DW, ODS, LZ SQL: 
```
psql -h localhost -U postgres -d postgres -f sql/ddl_dw.sql
psql -h localhost -U postgres -d postgres -f sql/ddl_ods.sql
psql -h localhost -U postgres -d postgres -f sql/ddl_lz.sql
```

## Airflow
- Place DAGs in `dags/`
- Webserver at `localhost:8080`
- Ensure connections `mysql_default` & `postgres_default` are configured
```
docker exec -it airflow airflow connections add 'mysql_default' \
    --conn-type 'mysql' \
    --conn-login 'user' \
    --conn-password 'db123' \
    --conn-host 'db-mysql' \
    --conn-port '3306' \
    --conn-schema 'ounass_source'
docker exec -it airflow airflow connections add 'postgres_default' \
  --conn-type 'postgres' \
  --conn-login 'postgres' \
  --conn-password 'pg123' \
  --conn-host 'db-postgres' \
  --conn-port '5432' \
  --conn-schema 'data_warehouse'
```

- Load variables.json airflow variables into airflow container:
```
docker cp variables.json airflow:/opt/airflow/variables.json
docker exec -it airflow airflow variables import /opt/airflow/variables.json
```

## Load initial data from ./data/
- convert data from .xlsx to .csv:
```
pip install openpyxl
cd data
python to_csv.py
cd ..
```

- load data into mysql_db container:
```bash
chmod +x load_mysql_data.sh
./load_mysql_data.sh
```
or in powershell: `.\load_mysql_data.ps1`

## Testing
```
pip install pytest
pytest tests/
```
