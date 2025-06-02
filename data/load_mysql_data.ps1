# Variables
$containerName = "mysql_db"
$dbName = "ounass_source"
$user = "root"
$password = "root123"

# Copy CSV files into container
docker cp "customer.csv" ${containerName}:"/tmp/customer.csv"
docker cp "salesorder.csv" ${containerName}:"/tmp/salesorder.csv"
docker cp "salesorderitem.csv" ${containerName}:"/tmp/salesorderitem.csv"

# Create SQL load script
$sqlScript = @"
USE $dbName;

LOAD DATA LOCAL INFILE '/tmp/customer.csv'
INTO TABLE customer
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/tmp/salesorder.csv'
INTO TABLE salesorder
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/tmp/salesorderitem.csv'
INTO TABLE salesorderitem
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
"@

# Write script to a temp file in the container
$tempSqlFile = "load_data.sql"
$sqlScript | Set-Content -Path $tempSqlFile -Encoding UTF8
docker cp $tempSqlFile ${containerName}:/tmp/$tempSqlFile

# Execute the SQL script
docker exec $containerName bash -c "mysql --local-infile=1 -u$user -p$password -D $dbName < /tmp/$tempSqlFile"
