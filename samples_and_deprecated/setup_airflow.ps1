# PowerShell Script to Fully Set Up Apache Airflow and Connections

# Abort on any error
$ErrorActionPreference = "Stop"

# === CONFIGURATION ===
$projectDir = "$PWD"
$pythonPath = "C:\Users\lenovo\AppData\Local\Programs\Python\Python310\python.exe"
$mysqlPassword = "db123"
$mysqlUser = "user"
$mysqlHost = "db-mysql"
$mysqlDB = "db"

$pgUser = "postgres"
$pgPassword = "pg123"
$pgHost = "db-postgres"
$pgDB = "postgres"

# === VIRTUAL ENV ===
Write-Host "`nCreating virtual environment..."
& ${pythonPath} -m venv .venv

Write-Host "Activating virtual environment..."
. .\.venv\Scripts\Activate.ps1

# === ENV VARS ===
Write-Host "Setting Airflow environment variables..."
$env:AIRFLOW_HOME = "${projectDir}\airflow"
$env:AIRFLOW__CORE__LOAD_EXAMPLES = "False"
$env:AIRFLOW_VERSION = "2.5.1"
$env:AIRFLOW_VARIABLES_FILEPATH = "./variables.json"
$env:PYTHON_VERSION = "3.10"

# === INSTALL BUILD TOOLS ===
Write-Host "`nInstalling BUILD TOOLS: wheel, setuptools"
pip install wheel setuptools

# === INSTALL AIRFLOW ===
Write-Host "`nInstalling Apache Airflow and providers..."
pip install "apache-airflow==$env:AIRFLOW_VERSION" `
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-$env:AIRFLOW_VERSION/constraints-$env:PYTHON_VERSION.txt"

pip install apache-airflow-providers-mysql `
            apache-airflow-providers-postgres `
            pandas mysqlclient psycopg2-binary `
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-$env:AIRFLOW_VERSION/constraints-$env:PYTHON_VERSION.txt"

# === INIT DB ===
Write-Host "`nInitializing Airflow DB..."
airflow db init

# === SET CONNECTIONS ===
Write-Host "`nSetting up Airflow connections..."
airflow connections add mysql_default `
  --conn-uri "mysql://${mysqlUser}:${mysqlPassword}@${mysqlHost}:3306/${mysqlDB}"

airflow connections add postgres_default `
  --conn-uri "postgresql://${pgUser}:${pgPassword}@${pgHost}:5432/${pgDB}"

# === SET VARIABLES ===
Write-Host "`nSetting up Airflow variables..."
airflow variables import $env:AIRFLOW_VARIABLES_FILEPATH

<#
# === CREATE ADMIN USER ===
Write-Host "`nCreating admin user..."
airflow users create `
  --username admin `
  --firstname Admin `
  --lastname User `
  --role Admin `
  --email admin@example.com `
  --password admin

# === START AIRFLOW ===
Write-Host "`nStarting Airflow Webserver (background)..."
Start-Process powershell -ArgumentList "airflow webserver" -WindowStyle Minimized

Start-Sleep -Seconds 5

Write-Host "Starting Airflow Scheduler (background)..."
Start-Process powershell -ArgumentList "airflow scheduler" -WindowStyle Minimized
#>

# === DONE ===
Write-Host "`nAirflow setup complete!"
<#
Write-Host "Open http://localhost:8080 to access the Airflow UI"
Write-Host "Login with: admin / admin"
Write-Host "Make sure your DAGs are inside: ${projectDir}\dags"
#>
