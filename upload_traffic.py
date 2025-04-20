import pandas as pd
from scrape_all_data import extract_traffic_incidents, transform_traffic_incidents
from bigquery_utils import (
    setup_bigquery_client,
    create_dataset_if_not_exists,
    upload_dataframe_to_bigquery,
    verify_upload
)

def fetch_and_upload_traffic_data(key_path, dataset_id="singapore_datasets"):
    """Fetch and upload traffic incidents data to BigQuery"""
    # Setup BigQuery client
    bq_client, project_id = setup_bigquery_client(key_path)
    
    # Create dataset if it doesn't exist
    create_dataset_if_not_exists(bq_client, project_id, dataset_id)
    
    # Extract traffic data
    print("=== EXTRACTING TRAFFIC INCIDENTS DATA ===")
    lta_account_key = "brlj8fVmS+u/X9vpSjismQ=="
    traffic_data = extract_traffic_incidents(lta_account_key)
    
    # Transform traffic data
    print("=== TRANSFORMING TRAFFIC INCIDENTS DATA ===")
    traffic_df = transform_traffic_incidents(traffic_data)
    
    # Upload traffic data to BigQuery
    print("=== UPLOADING TRAFFIC INCIDENTS DATA TO BIGQUERY ===")
    upload_dataframe_to_bigquery(
        bq_client,
        traffic_df,
        project_id,
        dataset_id,
        "traffic_incidents"
    )
    verify_upload(bq_client, project_id, dataset_id, "traffic_incidents")
    print(f"✅ traffic_incidents: {len(traffic_df)} rows uploaded successfully")
    
    print("=== TRAFFIC INCIDENTS DATA UPLOAD COMPLETE ===")

if __name__ == "__main__":
    # Path to your service account key file
    key_path = "./key/is3107-457309-0e9066063708.json"
    
    # Dataset name in BigQuery
    dataset_id = "singapore_datasets"
    
    # Fetch and upload traffic data
    fetch_and_upload_traffic_data(key_path, dataset_id)