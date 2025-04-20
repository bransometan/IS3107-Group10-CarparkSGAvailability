import pandas as pd
from scrape_all_data import extract_events_data, transform_events_data
from bigquery_utils import (
    setup_bigquery_client,
    create_dataset_if_not_exists,
    upload_dataframe_to_bigquery,
    verify_upload
)

def fetch_and_upload_events_data(key_path, dataset_id="singapore_datasets"):
    """Fetch and upload events data to BigQuery"""
    # Setup BigQuery client
    bq_client, project_id = setup_bigquery_client(key_path)
    
    # Create dataset if it doesn't exist
    create_dataset_if_not_exists(bq_client, project_id, dataset_id)
    
    # Extract events data
    print("=== EXTRACTING EVENTS DATA ===")
    stb_api_key = "r4VgRmHwVhs5uG1WlqXzprvFWCSK8lyf"
    events_data = extract_events_data(stb_api_key)
    
    # Transform events data
    print("=== TRANSFORMING EVENTS DATA ===")
    events_df = transform_events_data(events_data)
    
    # Upload events data to BigQuery
    print("=== UPLOADING EVENTS DATA TO BIGQUERY ===")
    upload_dataframe_to_bigquery(
        bq_client,
        events_df,
        project_id,
        dataset_id,
        "events"
    )
    verify_upload(bq_client, project_id, dataset_id, "events")
    print(f"✅ events: {len(events_df)} rows uploaded successfully")
    
    print("=== EVENTS DATA UPLOAD COMPLETE ===")

if __name__ == "__main__":
    # Path to your service account key file
    key_path = "./key/is3107-457309-0e9066063708.json"
    
    # Dataset name in BigQuery
    dataset_id = "singapore_datasets"
    
    # Fetch and upload events data
    fetch_and_upload_events_data(key_path, dataset_id)