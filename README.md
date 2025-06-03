# Ounass Data Engineering Case Study
## Setup
### In windows:
#### Run as administrator:
##### Install Docker:
      `.\docker\setup_docker.ps1`
      This will install and setup docker for first use
      Then log-out and log-in (or restart) to reflect docker settings

##### Run:
      `.\docker\run_docker.ps1`
      To run the docker containers or just do: `docker-compose up -d`

##### Stop:
      `.\docker\stop_docker.ps1`
      To stop the docker containers or just do: `docker-compose down`

##### Uninstall Docker:
      `.\docker\uninstall_docker.ps1`
      To uninstall docker from host system

### In Linux:
#### Install Docker:
    `./docker/setup_docker.sh`
    This will install and setup docker for first use
    Then log-out and log-in (or restart) to reflect docker settings

#### Run:
    `./docker/run_docker.sh`
    To run the docker containers or just do: `docker-compose up -d`

#### Stop:
    `./docker/stop_docker.sh`
    To stop the docker containers or just do: `docker-compose down`

#### Uninstall Docker:
    `./docker/uninstall_docker.sh`
    To uninstall docker from host system

## Testing
```
docker exec -it airflow-webserver bash
pip install pytest
pytest tests/
```
You can use `airflow-scheduler` or `airflow-init` instead of 
`airflow-webserver` as well

## Airflow helpers
- Place DAGs in `dags/`
- Webserver at `localhost:8080`
- Ensure connections `mysql_default` & `postgres_default` are configured
```
docker exec -it airflow-scheduler airflow connections delete 'mysql_default'
docker exec -it airflow airflow connections add 'mysql_default' \
    --conn-type 'mysql' \
    --conn-login 'user' \
    --conn-password 'db123' \
    --conn-host 'db-mysql' \
    --conn-port '3306' \
    --conn-schema 'ounass_source'
docker exec -it airflow-scheduler airflow connections delete 'postgres_default'
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
docker cp variables.json airflow-scheduler:/opt/airflow/variables.json
docker exec -it airflow-scheduler airflow variables import /opt/airflow/variables.json
```

## Initial data helpers from ./data/
- convert data from .xlsx to .csv:
```
pip install openpyxl
cd data
python to_csv.py
cd ..
```

## Others
1. Create Docker network: `docker network create etl_network`
2. `docker-compose up -d`
3. Load source SQL: `mysql -h localhost -P 3307 -u root -p < sql/ddl_source.sql`
or for powershell: `Get-Content sql\ddl_source.sql | mysql -h localhost -P 3307 -u root -p`
4. Load DW, ODS, LZ SQL: 
```
psql -h localhost -U postgres -d postgres -f sql/ddl_dw.sql
psql -h localhost -U postgres -d postgres -f sql/ddl_ods.sql
psql -h localhost -U postgres -d postgres -f sql/ddl_lz.sql
```
