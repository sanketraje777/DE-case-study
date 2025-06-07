#!/bin/bash

# Variables
CONTAINER_NAME="mysql_db"
DB_NAME="ounass_source"
MYSQL_USER="root"
MYSQL_PWD="root123"

# Copy CSV files to container
docker cp customer.csv $CONTAINER_NAME:/tmp/customer.csv
docker cp salesorder.csv $CONTAINER_NAME:/tmp/salesorder.csv
docker cp salesorderitem.csv $CONTAINER_NAME:/tmp/salesorderitem.csv

# Load data into MySQL
docker exec -i $CONTAINER_NAME bash -c "mysql --local-infile=1 -u$MYSQL_USER -p$MYSQL_PWD -D $DB_NAME <<EOF
LOAD DATA LOCAL INFILE '/tmp/customer.csv'
INTO TABLE customer
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/tmp/salesorder.csv'
INTO TABLE salesorder
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/tmp/salesorderitem.csv'
INTO TABLE salesorderitem
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
EOF"
