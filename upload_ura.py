import pandas as pd
from scrape_all_data import extract_ura_data, transform_ura_data
from bigquery_utils import (
    setup_bigquery_client,
    create_dataset_if_not_exists,
    upload_dataframe_to_bigquery,
    verify_upload
)

def fetch_and_upload_ura_data(key_path, dataset_id="singapore_datasets"):
    """Fetch and upload URA data to BigQuery"""
    # Setup BigQuery client
    bq_client, project_id = setup_bigquery_client(key_path)
    
    # Create dataset if it doesn't exist
    create_dataset_if_not_exists(bq_client, project_id, dataset_id)
    
    # Extract URA data
    print("=== EXTRACTING URA DATA ===")
    ura_access_key = "fb1d226d-6f7f-4ec4-b678-247dee275ca7"
    ura_data = extract_ura_data(ura_access_key)
    
    # Transform URA data
    print("=== TRANSFORMING URA DATA ===")
    ura_dfs = transform_ura_data(ura_data)
    
    # Upload URA data to BigQuery
    print("=== UPLOADING URA DATA TO BIGQUERY ===")
    
    # Upload URA carpark availability
    upload_dataframe_to_bigquery(
        bq_client,
        ura_dfs["availability"],
        project_id,
        dataset_id,
        "ura_carpark_availability"
    )
    verify_upload(bq_client, project_id, dataset_id, "ura_carpark_availability")
    print(f"✅ ura_carpark_availability: {len(ura_dfs['availability'])} rows uploaded successfully")
    
    # Upload URA carpark list
    upload_dataframe_to_bigquery(
        bq_client,
        ura_dfs["car_park_list"],
        project_id,
        dataset_id,
        "ura_carpark_list"
    )
    verify_upload(bq_client, project_id, dataset_id, "ura_carpark_list")
    print(f"✅ ura_carpark_list: {len(ura_dfs['car_park_list'])} rows uploaded successfully")
    
    # Upload URA season carpark list
    upload_dataframe_to_bigquery(
        bq_client,
        ura_dfs["season_car_park_list"],
        project_id,
        dataset_id,
        "ura_season_carpark_list"
    )
    verify_upload(bq_client, project_id, dataset_id, "ura_season_carpark_list")
    print(f"✅ ura_season_carpark_list: {len(ura_dfs['season_car_park_list'])} rows uploaded successfully")
    
    print("=== URA DATA UPLOAD COMPLETE ===")

if __name__ == "__main__":
    # Path to your service account key file
    key_path = "./key/is3107-457309-0e9066063708.json"
    
    # Dataset name in BigQuery
    dataset_id = "singapore_datasets"
    
    # Fetch and upload URA data
    fetch_and_upload_ura_data(key_path, dataset_id)