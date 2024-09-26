
from datetime import datetime, timedelta
from airflow.decorators import dag, task, task_group
from airflow.models.baseoperator import chain
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
import logging


# get the airflow.task logger
task_logger = logging.getLogger("airflow.task")

# Define the DAG function a set of parameters
@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    template_searchpath="/usr/local/airflow/include",
)
def etlflow_dag():
    t0 = EmptyOperator(task_id="start")
    @task
    def check_connection():
        task_logger.info("start checking connection...")
        mssql_hook = MsSqlHook(mssql_conn_id='mssql_conn', schema="AdventureWorksLT2022")
        my_records = mssql_hook.get_records("SELECT @@version, host_name()")
        print(my_records)
        task_logger.info("connection check completed.")

    @task_group
    def extract_data():
        from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
        import pandas as pd

        last_extract_date = "2008-02-01 00:00:00.000"

        # Establish connection to mssql
        mssql_hook = MsSqlHook(mssql_conn_id='mssql_conn', schema="AdventureWorksLT2022")

        @task
        def load_data_customer():
            # retrieve data from mssql
            df = mssql_hook.get_pandas_df(f"SELECT top(10) CustomerID, NameStyle,Title,FirstName, LastName, EmailAddress,Phone, modifieddate from AdventureWorksLT2022.SalesLT.Customer where modifieddate > '{last_extract_date}'")

            # Generate a new file name with the format customer_year_month_day_hour
            current_time = datetime.now()
            table_name = "customer"
            file_name = f"{table_name}_{current_time.year}_{current_time.month:02d}_{current_time.day:02d}_{current_time.hour:02d}.parquet"

            # save data to parquet file
            df.to_parquet(f"/usr/local/airflow/dags/files/{file_name}") # , compression='gzip'

        @task
        def load_data_product():
            # retrieve data from mssql
            df = mssql_hook.get_pandas_df(f"SELECT top(10) ProductID, Name, ProductNumber, Color, StandardCost, ListPrice, Size, Weight, modifieddate, ProductCategoryID, ProductModelID, SellStartDate, SellEndDate, DiscontinuedDate from AdventureWorksLT2022.SalesLT.Product where modifieddate > '{last_extract_date}'")

            # Generate a new file name with the format customer_year_month_day_hour
            current_time = datetime.now()
            table_name = "product"
            file_name = f"{table_name}_{current_time.year}_{current_time.month:02d}_{current_time.day:02d}_{current_time.hour:02d}.parquet"

            # save data to parquet file
            df.to_parquet(f"/usr/local/airflow/dags/files/{file_name}") # , compression='gzip'

        @task
        def load_data_salesOrderHeader():
            # retrieve data from mssql
            df = mssql_hook.get_pandas_df(f"SELECT top(10) SalesOrderID,RevisionNumber,OrderDate,DueDate,ShipDate,Status,OnlineOrderFlag,SalesOrderNumber,PurchaseOrderNumber,AccountNumber,CustomerID,ShipToAddressID,BillToAddressID,ShipMethod,CreditCardApprovalCode,SubTotal,TaxAmt,Freight,TotalDue,Comment,ModifiedDate from AdventureWorksLT2022.SalesLT.SalesOrderHeader where ModifiedDate > '{last_extract_date}'")

            # Generate a new file name with the format customer_year_month_day_hour
            current_time = datetime.now()
            table_name = "salesorderheader"
            file_name = f"{table_name}_{current_time.year}_{current_time.month:02d}_{current_time.day:02d}_{current_time.hour:02d}.parquet"

            # save data to parquet file
            df.to_parquet(f"/usr/local/airflow/dags/files/{file_name}") # , compression='gzip'

        @task
        def load_data_saleOrderDetail():
            # retrieve data from mssql
            df = mssql_hook.get_pandas_df(f"SELECT top(10) SalesOrderID,SalesOrderDetailID,OrderQty,ProductID,UnitPrice,UnitPriceDiscount,LineTotal,ModifiedDate from AdventureWorksLT2022.SalesLT.SalesOrderDetail where ModifiedDate > '{last_extract_date}'")

            # Generate a new file name with the format customer_year_month_day_hour
            current_time = datetime.now()
            table_name = "salesorderdetail"
            file_name = f"{table_name}_{current_time.year}_{current_time.month:02d}_{current_time.day:02d}_{current_time.hour:02d}.parquet"

            # save data to parquet file
            df.to_parquet(f"/usr/local/airflow/dags/files/{file_name}") # , compression='gzip'

        @task
        def load_data_address():
            # retrieve data from mssql
            df = mssql_hook.get_pandas_df(f"SELECT top(10) AddressID,AddressLine1,AddressLine2,City,StateProvince,CountryRegion,PostalCode,ModifiedDate from AdventureWorksLT2022.SalesLT.Address where ModifiedDate > '{last_extract_date}'")

            # Generate a new file name with the format customer_year_month_day_hour
            current_time = datetime.now()
            table_name = "address"
            file_name = f"{table_name}_{current_time.year}_{current_time.month:02d}_{current_time.day:02d}_{current_time.hour:02d}.parquet"

            # save data to parquet file
            df.to_parquet(f"/usr/local/airflow/dags/files/{file_name}") # , compression='gzip'


        chain(load_data_customer(), load_data_product(), load_data_salesOrderHeader(), load_data_saleOrderDetail(), load_data_address())

    @task
    def upload_using_wasb_connection():
        from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
        import os

        task_logger.info("start wasb connection...")

        # Establish connection to Azure Blob Storage
        wasb_hook = WasbHook(wasb_conn_id='azure_conn_constr')
        
        # Update the local file path with the new file name
        local_file_path = f'/usr/local/airflow/dags/files/'
        cloud_file_path = 'my-dev-data/uploaded-files'

        # Get all .parquet files in the local directory
        parquet_files = [f for f in os.listdir(local_file_path) if f.endswith('.parquet')]

        for file in parquet_files:
            task_logger.info(f"start uploading {file}...")
 
            # Check if the new file exists in Azure Blob Storage
            file_exists = wasb_hook.check_for_blob('my-dev-data/uploaded-files', file)
            
            if not file_exists:
                wasb_hook.load_file(f"{local_file_path}{file}", cloud_file_path, file)
                task_logger.info(f"File {file} uploaded to Azure Blob Storage")
            else:
                task_logger.info(f"File {file} already exists in Azure Blob Storage, skipping upload")
        
        task_logger.info("upload completed.")

    
    @task
    def clean_local_file():
        import os
        local_file_path = f'/usr/local/airflow/dags/files/'
        parquet_files = [f for f in os.listdir(local_file_path) if f.endswith('.parquet')]
        for file in parquet_files:
            os.remove(f"{local_file_path}{file}")
            task_logger.info(f"File {file} deleted from local directory")
    
    chain(t0, extract_data(), upload_using_wasb_connection(), clean_local_file())

etlflow_dag()