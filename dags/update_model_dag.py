from __future__ import annotations
import logging
import os
import pendulum 
from datetime import datetime, timedelta
import joblib
import numpy as np
import pandas as pd
from geopy.distance import geodesic

from airflow.decorators import dag, task
from airflow.models.param import Param
# from airflow.operators.python import get_current_context

from google.cloud import bigquery
from google.oauth2 import service_account

from sklearn.linear_model import SGDRegressor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

# import utils
from modules.ml_utils import create_feature_matrix
from modules.bigquery_utils import setup_bigquery_client, run_query


KEY_PATH = "/keys/is3107-457309-0e9066063708.json" 
PROJECT_ID = "is3107-457309" 
DATASET_ID = "singapore_datasets"
LOCAL_MODEL_DIR = "/model" 
LOCAL_MODEL_FILENAME = "model/sgd_regressor_pipeline.joblib"
LOCAL_MODEL_PATH = os.path.join(LOCAL_MODEL_DIR, LOCAL_MODEL_FILENAME)
TABLES = {
    "availability": "ura_carpark_availability",
    "list": "ura_carpark_list",
    "weather": "weather_rainfall", 
    "events": "events",
    "traffic": "traffic_incidents",
    "holidays": "public_holidays"
    # "season": "ura_season_carpark_list"
}

@dag(
    dag_id='carpark_model_weekly_update',
    schedule='@weekly', # update model once a week
    start_date=pendulum.datetime(2025, 4, 20, tz="Asia/Singapore"), 
    catchup=False, # Don't run for past missed schedules
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
    },
    tags=['project', 'ml', 'update'],
    description="Weekly update of the carpark availability prediction model using partial_fit.",
    params={ # params for manual run
        "start_date_override": Param(None, type=["null", "string"], description="Override start date. If None, use last week."),
        "end_date_override": Param(None, type=["null", "string"], description="Override end date. If None, use last week."),
    }
)
def model_update_dag():

    @task
    def get_update_time_range(**context):
        """Determines the start and end timestamps for the weekly data pull."""
        params = context["params"]
        logical_date = context["logical_date"] # pendulum

        if params.get("start_date_override") and params.get("end_date_override"):
            start_date = pendulum.parse(params["start_date_override"]).start_of('day')
            end_date = pendulum.parse(params["end_date_override"]).end_of('day')
            logging.info(f"Using override dates: {start_date.to_iso8601_string()} to {end_date.to_iso8601_string()}")
        else:
            end_date = logical_date.subtract(microseconds=1) 
            start_date = logical_date.subtract(weeks=1) 
            logging.info(f"Using last week's dates: {start_date.to_iso8601_string()} to {end_date.to_iso8601_string()}")

        # buffer window for weather / events / traffic
        buffer_start = start_date.subtract(hours=8)
        buffer_end = end_date.add(hours=1)

        return {
            "data_start_ts": start_date.to_iso8601_string(),
            "data_end_ts": end_date.to_iso8601_string(),
            "buffer_start_ts": buffer_start.to_iso8601_string(),
            "buffer_end_ts": buffer_end.to_iso8601_string(),
        }

    @task
    def query_weekly_data_from_bq(time_range: dict):
        """Queries BigQuery for data within the specified time range."""
        client, _ = setup_bigquery_client()
        data_start = time_range["data_start_ts"]
        data_end = time_range["data_end_ts"]
        buffer_start = time_range["buffer_start_ts"]
        buffer_end = time_range["buffer_end_ts"]

        fetched_data = {}

        query_avail = f"""
            SELECT *
            FROM `{PROJECT_ID}.{DATASET_ID}.{TABLES['availability']}`
            WHERE 
                TIMESTAMP(datetime) >= TIMESTAMP("{data_start}") AND 
                TIMESTAMP(datetime) < TIMESTAMP("{data_end}") AND
                lotType = "C"
        """
        fetched_data['availability'] = run_query(client, query_avail)
        logging.info(f"Fetched {len(fetched_data['availability'])} availability records.")
        if fetched_data['availability'].empty:
             logging.warning("No availability data found for the specified period.")

        query_weather = f"""
            SELECT 
                station_id,
                latitude,
                longitude,
                datetime,
                rainfall_amount
            FROM `{PROJECT_ID}.{DATASET_ID}.{TABLES['weather']}`
            WHERE 
                datetime >= TIMESTAMP("{buffer_start}") AND 
                datetime < TIMESTAMP("{buffer_end}")
        """
        fetched_data['weather'] = run_query(client, query_weather)
        logging.info(f"Fetched {len(fetched_data['weather'])} weather records.")

        query_events = f"""
            SELECT
                latitude,
                longitude,
                datetime
            FROM `{PROJECT_ID}.{DATASET_ID}.{TABLES['events']}`
        """
        fetched_data['events'] = run_query(client, query_events)
        fetched_data['events']['datetime'] = pd.to_datetime(fetched_data['events']['datetime']).dt.tz_localize('Asia/Singapore')
        # filter 
        fetched_data['events'] = fetched_data['events'][(
            fetched_data['events']['datetime'] >= pd.to_datetime(buffer_start).tz_convert('Asia/Singapore') ) &
            (fetched_data['events']['datetime'] < pd.to_datetime(buffer_end).tz_convert("Asia/Singapore")
        )]
        logging.info(f"Fetched {len(fetched_data['events'])} event records.")

        query_traffic = f"""
            SELECT
                latitude,
                longitude,
                incident_time
            FROM `{PROJECT_ID}.{DATASET_ID}.{TABLES['traffic']}`
            WHERE incident_time >= TIMESTAMP("{buffer_start}") AND incident_time < TIMESTAMP("{buffer_end}")
        """
        fetched_data['traffic'] = run_query(client, query_traffic)
        logging.info(f"Fetched {len(fetched_data['traffic'])} traffic records.")

        query_list = f"""
            SELECT 
                weekdayMin,
                weekdayRate,
                carparkNo,
                satdayMin,
                satdayRate,
                startTime,
                endTime,
                latitude,
                longitude,
                region
            FROM `{PROJECT_ID}.{DATASET_ID}.{TABLES['list']}`
            WHERE vehCat = "Car"
        """
        fetched_data['list'] = run_query(client, query_list)
        logging.info(f"Fetched {len(fetched_data['list'])} carpark list records.")

        query_holidays = f"""
            SELECT
                date
            FROM `{PROJECT_ID}.{DATASET_ID}.{TABLES['holidays']}`"""
        fetched_data['holidays'] = run_query(client, query_holidays)
        logging.info(f"Fetched {len(fetched_data['holidays'])} holiday records.")

        # Add ura_season_carpark_list if needed
        return fetched_data

    @task
    def check_data_and_model_file():
        """Checks if the local model file exists and if fetched data is present."""
        if not os.path.exists(LOCAL_MODEL_PATH):
            logging.error(f"Model file not found at {LOCAL_MODEL_PATH}. Initial training might be needed or path is incorrect.")
            raise FileNotFoundError(f"Model file not found at {LOCAL_MODEL_PATH}")
        return True # proceed

    @task
    def preprocess_new_data(data_dict: dict):
        """Preprocesses the fetched weekly data using the feature engineering logic."""
        if data_dict.get('availability', pd.DataFrame()).empty:
            logging.warning("Availability data is empty. Skipping preprocessing.")
            # return empty dataframes with correct columns to signal no update needed
            feature_cols = [
                "hour_of_day", "day_of_week", "is_weekend", "is_holiday",
                "rainfall", "is_traffic", "has_nearby_event",
                "total_lots", "parking_rate"
            ]
            return {"X_new": pd.DataFrame(columns=feature_cols), "y_new": pd.Series(dtype=float)}

        logging.info("Preprocessing new data...")
        feature_matrix = create_feature_matrix(data_dict)

        if feature_matrix.empty:
             logging.warning("Feature matrix is empty after processing. No data to update model with.")
             # return  empty dataframes with correct columns
             feature_cols = [
                "hour_of_day", "day_of_week", "is_weekend", "is_holiday",
                "rainfall", "is_traffic", "has_nearby_event",
                "total_lots", "parking_rate"
             ]
             return {"X_new": pd.DataFrame(columns=feature_cols), "y_new": pd.Series(dtype=float)}

        X_new = feature_matrix.drop(columns="utilisation_rate")
        y_new = feature_matrix["utilisation_rate"]

        logging.info(f"Preprocessing complete. Created {len(X_new)} new feature vectors.")
        return {"X_new": X_new, "y_new": y_new}

    @task
    def update_model_partial_fit(processed_data: dict):
        """Loads the model, performs partial_fit, and saves the updated model."""
        X_new = processed_data["X_new"]
        y_new = processed_data["y_new"]

        if X_new.empty or y_new.empty:
            logging.info("No new data to update the model with. Skipping partial_fit.")
            return "skipped"

        try:
            # Load the existing pipeline
            logging.info(f"Loading model pipeline from {LOCAL_MODEL_PATH}")
            pipeline = joblib.load(LOCAL_MODEL_PATH)
            logging.info("Model pipeline loaded successfully.")

            # Check if the loaded object is a Pipeline
            if not isinstance(pipeline, Pipeline):
                 raise TypeError(f"Loaded object is not a scikit-learn Pipeline: {type(pipeline)}")

            # Perform partial fit on new data
            logging.info(f"Performing partial_fit with {len(X_new)} samples...")
            pipeline.steps[-1][1].partial_fit(X_new, y_new)
            logging.info("Partial fit completed.")

            # Save
            os.makedirs(os.path.dirname(LOCAL_MODEL_PATH), exist_ok=True)
            logging.info(f"Saving updated model pipeline back to {LOCAL_MODEL_PATH}")
            joblib.dump(pipeline, LOCAL_MODEL_PATH)
            logging.info("Updated model saved successfully.")
            return "updated"

        except FileNotFoundError:
             logging.error(f"Model file {LOCAL_MODEL_PATH} not found during update task. This shouldn't happen if check_data_and_model_file passed.")
             raise # fail the task
        except Exception as e:
            logging.error(f"Error during model update/saving: {e}", exc_info=True)
            raise 

    # dependencies
    time_range_dict = get_update_time_range()
    can_proceed = check_data_and_model_file()
    raw_data_dict = query_weekly_data_from_bq(time_range_dict)

    # Ensure data query runs only if model file exists
    raw_data_dict.set_upstream(can_proceed)

    processed_data_dict = preprocess_new_data(raw_data_dict)
    update_status = update_model_partial_fit(processed_data_dict)


# Instantiate the DAG
model_update_dag_instance = model_update_dag()

