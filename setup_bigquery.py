from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd

# === Authentication ===
key_path = "./key/is3107-457309-0e9066063708.json"
credentials = service_account.Credentials.from_service_account_file(key_path)
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

# === Create dataset if not exists ===
project_id = credentials.project_id
dataset_id = "hdb_data"
full_dataset_id = f"{project_id}.{dataset_id}"

dataset = bigquery.Dataset(full_dataset_id)
dataset.location = "asia-southeast1"

client.create_dataset(dataset, exists_ok=True)
print(f"Dataset '{dataset_id}' is ready.")

# === Sample data ===
df = pd.DataFrame({
    "carpark_no": ["C1", "C2"],
    "agency": ["HDB", "URA"],
    "lots_available": [50, 100]
})

# === Define table and load config ===
table_id = "carpark_availability"
table_ref = f"{project_id}.{dataset_id}.{table_id}"

job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")

# === Upload DataFrame (auto-creates table and infers schema) ===
job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
job.result()

print(f"Uploaded {job.output_rows} rows to {table_ref}. Table auto-created if it didn't exist.")


query = f"""
SELECT *
FROM `{project_id}.{dataset_id}.{table_id}`
LIMIT 10
"""

# Run query
query_job = client.query(query)
results = query_job.result().to_dataframe()

# Display result
print(results)
