from datetime import datetime
import logging
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.models import Variable
import pandas as pd
import os

class cls_mssql:

    mssql_hook = None
    last_extract_date = None
    schema = "AdventureWorksLT2022"
    local_path = "/usr/local/airflow/dags/files/"
    def __init__(self, mssql_conn_id, schema):
        self.mssql_hook = MsSqlHook(mssql_conn_id=mssql_conn_id, schema=schema)
    
        # get last extract date from environment variable
        self.last_extract_date = Variable.get("last_extract_date")
        logging.info(f"last extract date: {self.last_extract_date}")

    def check_connection(self):
        try:
            logging.info("start checking connection...")
            logging.info(f"conn: {self.mssql_hook}")
            my_records = self.mssql_hook.get_records("SELECT @@version, host_name()")
            logging.info(my_records)
            logging.info("connection check completed.")
        except Exception as e:
            logging.error(f"Failed to connect to Azure Blob Storage: {e}")
            raise e
        
    def get_file_name(self, table_name):
        current_time = datetime.now()
        file_name = f"{table_name}_{current_time.year}_{current_time.month:02d}_{current_time.day:02d}_{current_time.hour:02d}.parquet"
        return file_name

    def get_records(self, database_name, schema_name, table_name, columns):
        try:
            # retrieve data from mssql
            query = f"SELECT top(10) {columns} from {database_name}.{schema_name}.{table_name} where modifieddate > '{self.last_extract_date}'"
            logging.info(f"Debug: query: {query}")
            df = self.mssql_hook.get_pandas_df(query)
            return df
        except Exception as e:
            print(e)
            raise e

        

    def set_parquet_file(self, df, table_name):
        try:
            # get full file name
            full_file_name = f"{self.local_path}{self.get_file_name(table_name)}"

            # save data to parquet file
            df.to_parquet(full_file_name) # , compression='gzip'
        except Exception as e:
            print(e)


    