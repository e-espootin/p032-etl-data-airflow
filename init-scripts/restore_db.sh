# Inside the init-scripts folder, create a file named `restore_db.sh` with the following content:

# /init-scripts/restore_db.sh
# --------------------------------------
#!/bin/bash

# Download the backup file from the provided URL

#curl -o /var/opt/mssql/backup/AdventureWorksLT2022.bak "https://github.com/Microsoft/sql-server-samples/releases/download/adventureworks/AdventureWorksLT2022.bak"
#wget https://github.com/Microsoft/sql-server-samples/releases/download/adventureworks/AdventureWorksLT2022.bak /var/opt/mssql/backup/AdventureWorksLT2022.bak
#wget https://github.com/Microsoft/sql-server-samples/releases/download/adventureworks/AdventureWorksLT2022.bak /tmp/backup.bak 


# Wait for SQL Server to start
echo 'log 0'
sleep 20s
echo 'Hello!'
echo 'log 1'
###
/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'w&P,i8]69qO8XN' -Q 'select name from sys.databases' -C

# Restore the database from the backup file
/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "w&P,i8]69qO8XN" -C -Q "RESTORE DATABASE AdventureWorksLT2022 FROM DISK = '/var/opt/mssql/scripts/AdventureWorksLT2022.bak' WITH MOVE 'AdventureWorksLT2022_Data' TO '/var/opt/mssql/data/AdventureWorksLT2022.mdf', MOVE 'AdventureWorksLT2022_Log' TO '/var/opt/mssql/data/YourDatabaseName_log.ldf';"
# /opt/mssql-tools/bin/sqlcmd
# WITH MOVE 'YourDataLogicalName' TO '/var/opt/mssql/data/AdventureWorksLT2022.mdf', MOVE 'YourLogLogicalName' TO '/var/opt/mssql/data/YourDatabaseName_log.ldf'

/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'w&P,i8]69qO8XN' -Q 'select name from sys.databases' -C


echo 'restore is done!'

#test

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

sleep 10s

/opt/mssql/bin/sqlservr 
# --------------------------------------

# Notes:
# - Replace the URL in the `curl` command with the actual URL of your backup file.
# - Replace `YourDatabaseName`, `YourDataLogicalName`, and `YourLogLogicalName` with the actual names used in your backup.