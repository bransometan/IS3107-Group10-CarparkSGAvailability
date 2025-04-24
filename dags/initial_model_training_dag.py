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
from dags.modules.ml_utils import  (
    create_feature_matrix,
    initial_train_and_save
)
from dags.modules.bigquery_utils import (
    setup_bigquery_client, run_query
)

KEY_PATH = "/keys/is3107-457309-0e9066063708.json" 
PROJECT_ID = "is3107-457309" 
DATASET_ID = "singapore_datasets"
LOCAL_MODEL_DIR = "/model" 
LOCAL_MODEL_FILENAME = "model/sgd_regressor_pipeline.joblib"
LOCAL_MODEL_PATH = os.path.join(LOCAL_MODEL_DIR, LOCAL_MODEL_FILENAME)
TABLES = TABLES = {
    "availability": "ura_carpark_availability",
    "list": "ura_carpark_list",
    "weather": "weather_rainfall", 
    "events": "events",
    "traffic": "traffic_incidents",
    "holidays": "public_holidays"
    # "season": "ura_season_carpark_list"
}

@dag(
    dag_id='carpark_model_initial_training',
    schedule=None, # manual
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Singapore"),
    catchup=False,
    tags=['project', 'ml', 'initial_train'],
    description="One-time / occassional initial training of the model."
)
def initial_training_dag():

    @task
    def query_all_historical_data():
        """
        Queries BigQuery for data within the specified time range (start of yr
        to 1 wk ago).
        """
        client, _ = setup_bigquery_client(KEY_PATH)
        start_date_pend = pendulum.parse("2025-01-01T00:00:00+08:00")
        end_date_pend = pendulum.now("Asia/Singapore").subtract(weeks=1)
        buffer_start_pend = start_date_pend.subtract(hours=8)
        buffer_end_pend = end_date_pend.subtract(hours=1)

        data_start = start_date_pend.to_iso8601_string() # assume start of data is beginning of yr
        data_end = end_date_pend.to_iso8601_string() # train until data from 1 wk ago
        buffer_start = buffer_start_pend.to_iso8601_string()
        buffer_end = buffer_end_pend.to_iso8601_string()     

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
                region,
                parkCapacity
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

        # ura_season_carpark_list
        return fetched_data

    @task
    def preprocess_and_train(data_dict: dict):
        """Preprocesses the initial data using the same logic."""

        logging.info("Preprocessing new data...")
        feature_matrix = create_feature_matrix(data_dict)

        if feature_matrix.empty:
              raise ValueError("Feature matrix is empty. Cannot train.")

        X_train = feature_matrix.drop(columns="utilisation_rate")
        y_train = feature_matrix["utilisation_rate"]

        logging.info(f"Preprocessing complete. Created {len(X_train)} new feature vectors.")
        initial_train_and_save(X_train, y_train, LOCAL_MODEL_PATH) # Pass the path


    # Define dependencies
    raw_data = query_all_historical_data()
    preprocess_and_train(raw_data)

# Instantiate the DAG
initial_training_dag_instance = initial_training_dag()

