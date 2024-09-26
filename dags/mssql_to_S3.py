from airflow import DAG
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from pendulum import datetime
import pandas as pd
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.operators.python_operator import PythonOperator
import logging

from airflow.providers.amazon.aws.hooks.s3 import S3Hook



# from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
#from airflow.providers.microsoft.azure.hooks.azure_data_lake_hook import AzureDataLakeHook

# get the airflow.task logger
task_logger = logging.getLogger("airflow.task")


# Define the DAG function a set of parameters
@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    template_searchpath="/usr/local/airflow/include",
)
def etlflow_dag_S3():
    @task
    def load_data():
        # Establish connection to mssql
        mssql_hook = MsSqlHook(mssql_conn_id='mssql_conn', schema="AdventureWorksLT2022")
        
        # # All the regular dbapihook methods works
        my_records = mssql_hook.get_records("SELECT @@version, host_name()")
        # mssql.run("DELETE FROM othet_staging_table_name")
        print(my_records)

        # This method (get_pandas_df) does not work with the regular mssql plugin
        df = mssql_hook.get_pandas_df("SELECT top(10) * from AdventureWorksLT2022.SalesLT.Customer")
        print(df.count)

        # Saving data to a staging table using pandas to_sql
        # conn = mssql.get_sqlalchemy_engine()
        # df.to_sql("staging_table_name", con=conn, if_exists="replace")

        

        # put data in data lake
        s3 = S3Hook(aws_conn_id='aws_conn')
        task_logger.info("log 1")
        s3.load_string(string_data='enter value 2', key='file3.txt', bucket_name='ebi-generalpurpose-bucket', replace=True)

    

    chain(load_data())

etlflow_dag_S3()