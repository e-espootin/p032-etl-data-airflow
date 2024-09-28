from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
import os
import logging

# get the airflow.task logger
task_logger = logging.getLogger("airflow.task")

class cls_upload_parquet_to_azure:
    wasb_hook = None
    container_name = None
    local_file_path = None
    cloud_file_path = None

    def __init__(self, wasb_conn_id, container_name, local_file_path, cloud_file_path):
        self.wasb_hook = WasbHook(wasb_conn_id=wasb_conn_id)
        self.container_name = container_name
        self.local_file_path = local_file_path
        self.cloud_file_path = cloud_file_path


    def upload_parquet_file(self):
        try:
            # Update the local file path with the new file name
            local_file_path = f'/usr/local/airflow/dags/files/'
            cloud_file_path = 'my-dev-data/uploaded-files'

            # Get all .parquet files in the local directory
            parquet_files = [f for f in os.listdir(self.local_file_path) if f.endswith('.parquet')]


            for file in parquet_files:
                logging.info(f"start uploading {file}...")

                # Check if the new file exists in Azure Blob Storage
                file_exists = self.wasb_hook.check_for_blob(cloud_file_path, file)

                if not file_exists:
                    self.wasb_hook.load_file(f"{local_file_path}{file}", cloud_file_path, file)
                    logging.info(f"File {file} uploaded to Azure Blob Storage")
                else:
                    logging.info(f"File {file} already exists in Azure Blob Storage, skipping upload")
        except Exception as e:
            logging.error(f"Failed to upload parquet file: {e}")

        
    
