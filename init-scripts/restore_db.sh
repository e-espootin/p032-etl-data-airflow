#!/bin/bash

# Download the backup file from the provided URL
#curl -o /var/opt/mssql/backup/AdventureWorksLT2022.bak "https://github.com/Microsoft/sql-server-samples/releases/download/adventureworks/AdventureWorksLT2022.bak"
#wget https://github.com/Microsoft/sql-server-samples/releases/download/adventureworks/AdventureWorksLT2022.bak /var/opt/mssql/backup/AdventureWorksLT2022.bak


# Wait for SQL Server to start
sleep 20s
echo 'restore db...'
#echo 'mssql_user: ' ${mssql_user}
#echo 'mssql_password: ' ${mssql_password}

###
/opt/mssql-tools18/bin/sqlcmd -S localhost -U ${mssql_user} -P ${mssql_password} -Q 'select name from sys.databases' -C

# Restore the database from the backup file
/opt/mssql-tools18/bin/sqlcmd -S localhost -U ${mssql_user} -P ${mssql_password} -C -Q "RESTORE DATABASE AdventureWorksLT2022 FROM DISK = '/var/opt/mssql/scripts/AdventureWorksLT2022.bak' WITH MOVE 'AdventureWorksLT2022_Data' TO '/var/opt/mssql/data/AdventureWorksLT2022.mdf', MOVE 'AdventureWorksLT2022_Log' TO '/var/opt/mssql/data/YourDatabaseName_log.ldf';"


# test
/opt/mssql-tools18/bin/sqlcmd -S localhost -U ${mssql_user} -P ${mssql_password} -Q 'select name from sys.databases' -C


echo 'restore is done!'



# Find the process ID of sqlservr
pid=$(pgrep -f /opt/mssql/bin/sqlservr)

# Check if the process is running
if [ -n "$pid" ]; then
  echo "Stopping sqlservr process with PID: $pid"
  kill $pid
  # Optional: Wait for the process to actually stop
  wait $pid 2>/dev/null
  echo "sqlservr process stopped."
else
  echo "sqlservr process not found."
fi

# wait to stop complete
sleep 10s

# start sql server
/opt/mssql/bin/sqlservr 
# --------------------------------------
