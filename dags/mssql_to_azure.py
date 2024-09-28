
from datetime import datetime, timedelta
from airflow.decorators import dag, task, task_group
from airflow.models.baseoperator import chain
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
import logging
from scripts.cls_mssql import cls_mssql
from scripts.cls_upload_parquet_to_azure import cls_upload_parquet_to_azure
# get the airflow.task logger
task_logger = logging.getLogger("airflow.task")

database_name = "AdventureWorksLT2022"
# Define the DAG function a set of parameters
@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    template_searchpath="/usr/local/airflow/include",
)
def etlflow_dag():
    @task
    def start_dag():
        task_logger.info("start...")

    @task
    def check_connection():
        mssql_obj = cls_mssql(mssql_conn_id='mssql_conn', schema=database_name)
        mssql_obj.check_connection()

    @task_group
    def extract_data():
        @task
        def load_data_customer():
            mssql_obj = cls_mssql(mssql_conn_id='mssql_conn', schema=database_name)
            logging.info(f"Debug: mssql_obj: {mssql_obj}")
            table_name = "customer"
            logging.info(f"Debug: mssql_obj: {table_name}")
            df = mssql_obj.get_records(database_name, 'SalesLT', table_name, "CustomerID, NameStyle,Title,FirstName, LastName, EmailAddress,Phone, modifieddate")
            mssql_obj.set_parquet_file(df, table_name)
            
        @task
        def load_data_product():
            mssql_obj = cls_mssql(mssql_conn_id='mssql_conn', schema=database_name)
            table_name = "Product"
            df = mssql_obj.get_records(database_name, 'SalesLT', table_name, "ProductID, Name, ProductNumber, Color, StandardCost, ListPrice, Size, Weight, modifieddate, ProductCategoryID, ProductModelID, SellStartDate, SellEndDate, DiscontinuedDate")
            mssql_obj.set_parquet_file(df, table_name)

        @task
        def load_data_salesOrderHeader():
            mssql_obj = cls_mssql(mssql_conn_id='mssql_conn', schema=database_name)
            table_name = "SalesOrderHeader"
            df = mssql_obj.get_records(database_name, 'SalesLT', table_name, "SalesOrderID,RevisionNumber,OrderDate,DueDate,ShipDate,Status,OnlineOrderFlag,SalesOrderNumber,PurchaseOrderNumber,AccountNumber,CustomerID,ShipToAddressID,BillToAddressID,ShipMethod,CreditCardApprovalCode,SubTotal,TaxAmt,Freight,TotalDue,Comment,ModifiedDate")
            mssql_obj.set_parquet_file(df, table_name)

        @task
        def load_data_saleOrderDetail():
            mssql_obj = cls_mssql(mssql_conn_id='mssql_conn', schema=database_name)
            table_name = "SalesOrderDetail"
            df = mssql_obj.get_records(database_name, 'SalesLT', table_name, "SalesOrderID,SalesOrderDetailID,OrderQty,ProductID,UnitPrice,UnitPriceDiscount,LineTotal,ModifiedDate")
            mssql_obj.set_parquet_file(df, table_name)


        @task
        def load_data_address():
            mssql_obj = cls_mssql(mssql_conn_id='mssql_conn', schema=database_name)
            table_name = "Address"
            df = mssql_obj.get_records(database_name, 'SalesLT', table_name, "AddressID,AddressLine1,AddressLine2,City,StateProvince,CountryRegion,PostalCode,ModifiedDate")
            mssql_obj.set_parquet_file(df, table_name)


        chain(load_data_customer(), load_data_product(), load_data_salesOrderHeader(), load_data_saleOrderDetail(), load_data_address())

    @task
    def upload_using_wasb_connection():

        task_logger.info("start uploading...")

        upload_obj = cls_upload_parquet_to_azure(wasb_conn_id='azure_conn', container_name='my-dev-data', local_file_path='/usr/local/airflow/dags/files/', cloud_file_path='my-dev-data/uploaded-files')
        upload_obj.upload_parquet_file()

        task_logger.info("upload completed.")

    
    @task
    def clean_local_file():
        import os
        local_file_path = f'/usr/local/airflow/dags/files/'
        parquet_files = [f for f in os.listdir(local_file_path) if f.endswith('.parquet')]
        for file in parquet_files:
            os.remove(f"{local_file_path}{file}")
            task_logger.info(f"File {file} deleted from local directory")

        # keep the last extract date 
        Variable.set(key="last_extract_date", value=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        task_logger.info(f"saved the last extract date: {Variable.get('last_extract_date')}")
    
    chain(start_dag(), check_connection(), extract_data(), upload_using_wasb_connection(), clean_local_file())

etlflow_dag()