# setup_bigquery.py

import time
import pandas as pd
from scrape_all_data import fetch_all_data
# Import functions from bigquery_utils
from bigquery_utils import (
    setup_bigquery_client,
    create_dataset_if_not_exists,
    upload_dataframe_to_bigquery,
    verify_upload
)

def upload_all_data_to_bigquery(dataframes_dict, key_path, dataset_id="singapore_data"):
    """Upload all dataframes to BigQuery"""
    # Setup BigQuery client
    bq_client, project_id = setup_bigquery_client(key_path)
    
    # Create dataset if it doesn't exist
    create_dataset_if_not_exists(bq_client, project_id, dataset_id)
    
    # Upload each dataframe
    print("=== UPLOADING TO BIGQUERY ===")
    upload_results = {}
    
    for table_id, df in dataframes_dict.items():
        try:
            print(f"Uploading {table_id}...")
            table_ref = upload_dataframe_to_bigquery(
                bq_client, 
                df, 
                project_id, 
                dataset_id, 
                table_id
            )
            verify_upload(bq_client, project_id, dataset_id, table_id)
            upload_results[table_id] = {"status": "success", "rows": len(df)}
        except Exception as e:
            print(f"Error uploading {table_id}: {str(e)}")
            upload_results[table_id] = {"status": "error", "message": str(e)}
    
    print("=== UPLOAD SUMMARY ===")
    for table, result in upload_results.items():
        status = result["status"]
        if status == "success":
            print(f"✅ {table}: {result['rows']} rows uploaded successfully")
        else:
            print(f"❌ {table}: Failed - {result['message']}")
    
    return upload_results

def run_scheduled_upload(key_path, dataset_id="singapore_data", interval_hours=1):
    """Run scheduled uploads at specified interval"""
    while True:
        print(f"\n=== STARTING DATA COLLECTION AND UPLOAD AT {time.strftime('%Y-%m-%d %H:%M:%S')} ===")
        
        # Fetch all data
        dataframes = fetch_all_data()
        
        # Upload to BigQuery
        upload_all_data_to_bigquery(dataframes, key_path, dataset_id)
        
        next_run = time.strftime('%Y-%m-%d %H:%M:%S', 
                                time.localtime(time.time() + interval_hours * 3600))
        print(f"=== UPLOAD COMPLETE. NEXT RUN SCHEDULED AT {next_run} ===")
        
        # Sleep until next run
        time.sleep(interval_hours * 3600)

# ===== MAIN EXECUTION FUNCTION =====

def main():
    """Main execution function for BigQuery upload"""
    # Path to your service account key file
    key_path = "../../key/is3107-457309-0e9066063708.json"
    
    # Dataset name in BigQuery
    dataset_id = "singapore_datasets"
    
    # To run once:
    print("Fetching data...")
    dataframes = fetch_all_data()
    
    print("Uploading to BigQuery...")
    upload_all_data_to_bigquery(dataframes, key_path, dataset_id)
    
    # To run on a schedule, uncomment this line and comment the above lines:
    # run_scheduled_upload(key_path, dataset_id, interval_hours=1)  # Run every hour
    
    print("=== PROCESS COMPLETE ===")

if __name__ == "__main__":
    main()