#bigquery_utils.py

from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd

def setup_bigquery_client(key_path):
    """Setup and return BigQuery client"""
    credentials = service_account.Credentials.from_service_account_file(key_path)
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    return client, credentials.project_id

def create_dataset_if_not_exists(client, project_id, dataset_id, location="asia-southeast1"):
    """Create a dataset if it doesn't exist"""
    full_dataset_id = f"{project_id}.{dataset_id}"
    dataset = bigquery.Dataset(full_dataset_id)
    dataset.location = location
    client.create_dataset(dataset, exists_ok=True)
    print(f"Dataset '{dataset_id}' is ready.")
    return full_dataset_id

def upload_dataframe_to_bigquery(client, df, project_id, dataset_id, table_id, write_disposition="WRITE_APPEND"):
    """Upload a DataFrame to BigQuery"""
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    
    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
         # This is the key setting that allows schema updates:
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
        ],
        # Auto-detect the schema from the DataFrame
        autodetect=True
    )
    
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  # Wait for the job to complete
    
    print(f"Uploaded {job.output_rows} rows to {table_ref}")
    return table_ref

def verify_upload(client, project_id, dataset_id, table_id, limit=5):
    """Verify the upload by querying the table"""
    query = f"""
    SELECT *
    FROM `{project_id}.{dataset_id}.{table_id}`
    LIMIT {limit}
    """
    
    query_job = client.query(query)
    results = query_job.result().to_dataframe()
    
    print(f"\nVerifying upload to {table_id} (sample of {limit} rows):")
    print(results)
    
    return results

def run_query(client, query):
    """Run a BigQuery query and return results as a dataframe"""
    try:
        return client.query(query).to_dataframe()
    except Exception as e:
        print(f"Error running query: {str(e)}")
        return pd.DataFrame()
    