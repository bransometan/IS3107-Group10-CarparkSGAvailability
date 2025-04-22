import pandas as pd
from scrape_all_data import extract_sg_public_holidays, transform_sg_public_holidays
from bigquery_utils import (
    setup_bigquery_client,
    create_dataset_if_not_exists,
    upload_dataframe_to_bigquery,
    verify_upload
)

def fetch_and_upload_holiday_data(key_path, dataset_id="singapore_datasets"):
    """Fetch and upload public holiday data to BigQuery"""
    # Setup BigQuery client
    bq_client, project_id = setup_bigquery_client(key_path)
    
    # Create dataset if it doesn't exist
    create_dataset_if_not_exists(bq_client, project_id, dataset_id)
    
    # Extract holiday data
    print("=== EXTRACTING PUBLIC HOLIDAY DATA ===")
    sg_holiday_dataset_ids = [
        "d_3751791452397f1b1c80c451447e40b7",
        "d_4e19214c3a5288eab7a27235f43da4fa",
        "d_0b0fe4a7101674be0a359e55d32cd126"
    ]
    holiday_data = extract_sg_public_holidays(sg_holiday_dataset_ids)
    
    # Transform holiday data
    print("=== TRANSFORMING PUBLIC HOLIDAY DATA ===")
    holiday_df = transform_sg_public_holidays(holiday_data)
    
    # Upload holiday data to BigQuery
    print("=== UPLOADING PUBLIC HOLIDAY DATA TO BIGQUERY ===")
    upload_dataframe_to_bigquery(
        bq_client,
        holiday_df,
        project_id,
        dataset_id,
        "public_holidays"
    )
    verify_upload(bq_client, project_id, dataset_id, "public_holidays")
    print(f"✅ public_holidays: {len(holiday_df)} rows uploaded successfully")
    
    print("=== PUBLIC HOLIDAY DATA UPLOAD COMPLETE ===")

if __name__ == "__main__":
    # Path to your service account key file
    key_path = "../../key/is3107-457309-0e9066063708.json"
    
    # Dataset name in BigQuery
    dataset_id = "singapore_datasets"
    
    # Fetch and upload holiday data
    fetch_and_upload_holiday_data(key_path, dataset_id)