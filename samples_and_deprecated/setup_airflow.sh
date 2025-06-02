#!/bin/bash

# Exit on any error
set -e

# === CONFIGURATION ===
PYTHON_PATH="/usr/bin/python3.10"       # Adjust if Python 3.10 is elsewhere
PROJECT_DIR=$(pwd)
MYSQL_USER="user"
MYSQL_PASSWORD="db123"
MYSQL_HOST="db-mysql"
MYSQL_DB="db"

PG_USER="postgres"
PG_PASSWORD="pg123"
PG_HOST="db-postgres"
PG_DB="postgres"

AIRFLOW_VERSION="2.5.1"
PYTHON_VERSION="3.10"
AIRFLOW_VARIABLES_FILEPATH="./variables.json"

# === VENV SETUP ===
echo "Creating virtual environment..."
$PYTHON_PATH -m venv .venv

echo "Activating virtual environment..."
source .venv/bin/activate

# === ENV VARS ===
export AIRFLOW_HOME="$PROJECT_DIR/airflow"
export AIRFLOW__CORE__LOAD_EXAMPLES="False"

# === INSTALL BUILD TOOLS ===
echo "Installing BUILD TOOLS: wheel, setuptools"
pip install wheel setuptools

# === INSTALL AIRFLOW ===
echo "Installing Apache Airflow and providers..."
pip install "apache-airflow==$AIRFLOW_VERSION" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install apache-airflow-providers-mysql \
            apache-airflow-providers-postgres \
            pandas mysqlclient psycopg2-binary \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# === INITIALIZE DB ===
echo "Initializing Airflow DB..."
airflow db init

# === CREATE CONNECTIONS ===
echo "Creating Airflow connections..."
airflow connections add mysql_default \
  --conn-uri "mysql://${MYSQL_USER}:${MYSQL_PASSWORD}@${MYSQL_HOST}:3306/${MYSQL_DB}"

airflow connections add postgres_default \
  --conn-uri "postgresql://${PG_USER}:${PG_PASSWORD}@${PG_HOST}:5432/${PG_DB}"

# === SET VARIABLES ===
echo "Setting up Airflow variables..."
airflow variables import ${AIRFLOW_VARIABLES_FILEPATH}

./variables.json

: <<'COMMENTED'
# === CREATE ADMIN USER ===
echo "Creating admin user..."
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin

# === START AIRFLOW ===
echo "Starting Airflow Webserver in background..."
airflow webserver -p 8080 &

sleep 5

echo "Starting Airflow Scheduler in background..."
airflow scheduler &
COMMENTED

echo ""
echo "Setup complete!"
: <<'COMMENTED'
echo "Open http://localhost:8080 to access the Airflow UI"
echo "Login with: admin / admin"
COMMENTED
