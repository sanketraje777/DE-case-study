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
### Running pytest
```
docker exec -it airflow-webserver bash
pip install pytest
pytest tests/
```
You can use `airflow-scheduler` or `airflow-init` instead of 
`airflow-webserver` as well

## Running DAG's from Airflow UI
- Go to `localhost:8080` in your web browser
- Type username: `admin` and password: `admin` and press enter
- You will see 3 DAG's representing each phase of ETL
- To test the DAG's, unpause the DAG's and `etl_landing` will automatically start running
- The DAG's are chained, so `etl_landing` -> `etl_ods` -> `etl_dm`
- You can view the logs, graphs and other metrics through the Airflow UI

### The etl_landing DAG has 3 configurable methods of data extraction and loading
#### `./dags/etl_landing/data.py` (Default): 
This handles task distribution using LIMIT/OFFSET chunks
#### `./dags/etl_landing/data_keyset.py`: 
This handles task distribution using keyset pagination by modified_at datetime on salesorderitem & salesorder tables and by id values on customer table.
#### `./dags/etl_landing/data_keyset2.py`: 
This handles task distribution using keyset pagination solely on primary keys of the 3 tables which are the id fields (customer.id, salesorder.id, salesorderitem.item_id)

- You can create more data_*.py files to generate your own configurable methods for any of the 3 DAG's
- By default, `etl_landing/dag.py` imports `data.py`, but you can modify the import statement of `etl_landing/dag.py` to import the other configurations like `data_keyset.py` or `data_keyset2.py` like so:
```
from etl_landing.data_keyset import sql
```
or
```
from etl_landing.data_keyset2 import sql
```

## variables.json
- By default, the `variables_testing.json` from project root directory of the host system is loaded into airflow
- A real-world variables config file `variables.json` has already been provided in the project root directory of the host system
- You can upload this `variables.json` file or any other variables config file or add/delete/update individual variables as well

## Data Analysis
### Install required libraries
```
pip install -r requirements.txt
```  
or
```
pip install jupyterlab pandas sqlalchemy psycopg2-binary matplotlib seaborn scikit-learn pyarrow fastparquet ydata-profiling ipywidgets
```  

### Run Data Analysis pipeline
- open ./analysis/analysis.ipynb or corresponding ./analysis/analysis.py file and run all cells
- the ./analysis/outputs/ folder consists of important processed data/outputs saved into this folder when the .ipynb or .py file runs

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
psql -h localhost -U postgres -d data_warehouse -f sql/ddl_dw.sql
psql -h localhost -U postgres -d data_warehouse -f sql/ddl_ods.sql
psql -h localhost -U postgres -d data_warehouse -f sql/ddl_lz.sql
```
