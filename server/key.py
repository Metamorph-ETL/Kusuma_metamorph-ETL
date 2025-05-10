from google.cloud import storage
from datetime import datetime

# Path to your downloaded service account key
key_path = r"D:\ETL_pipeline_project\server\meta-morph-d-eng-pro-view-key.json"

# Initialize a GCS client
client = storage.Client.from_service_account_json(key_path)

