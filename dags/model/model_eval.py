# model eval;

from __future__ import annotations
import os
import pendulum 
import joblib
import pandas as pd
import sys
from sklearn.linear_model import SGDRegressor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

# import utils
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../modules')))
from ml_utils import  (
    create_feature_matrix,
    initial_train_and_save
)
from bigquery_utils import (
    setup_bigquery_client, run_query
)

script_dir = os.path.dirname(os.path.abspath(__file__))
KEY_PATH = os.path.join(script_dir, '..', '..', 'key', 'is3107-457309-0e9066063708.json')
PROJECT_ID = "is3107-457309" 
DATASET_ID = "singapore_datasets"
LOCAL_MODEL_PATH = "sgd_regressor_pipeline.joblib"
TABLES = TABLES = {
    "availability": "ura_carpark_availability",
    "list": "ura_carpark_list",
    "weather": "weather_rainfall", 
    "events": "events",
    "traffic": "traffic_incidents",
    "holidays": "public_holidays"
    # "season": "ura_season_carpark_list"
}

def query_all_historical_data():
    """
    Queries BigQuery for data within the specified time range (start of yr
    to 3 days ago).
    """
    client, _ = setup_bigquery_client(KEY_PATH)
    start_date_pend = pendulum.parse("2025-01-01T00:00:00+08:00")
    end_date_pend = pendulum.now("Asia/Singapore").subtract(days=3)
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

    query_events = f"""
        SELECT
            latitude,
            longitude,
            datetime
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLES['events']}`
    """
    fetched_data['events'] = run_query(client, query_events)
    print(fetched_data['events'])
    fetched_data['events']['datetime'] = pd.to_datetime(fetched_data['events']['datetime']).dt.tz_localize('Asia/Singapore')
    # filter 
    fetched_data['events'] = fetched_data['events'][(
        fetched_data['events']['datetime'] >= pd.to_datetime(buffer_start).tz_convert('Asia/Singapore') ) &
        (fetched_data['events']['datetime'] < pd.to_datetime(buffer_end).tz_convert("Asia/Singapore")
    )]

    query_traffic = f"""
        SELECT
            latitude,
            longitude,
            incident_time
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLES['traffic']}`
        WHERE incident_time >= TIMESTAMP("{buffer_start}") AND incident_time < TIMESTAMP("{buffer_end}")
    """
    fetched_data['traffic'] = run_query(client, query_traffic)

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

    query_holidays = f"""
        SELECT
            date
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLES['holidays']}`"""
    fetched_data['holidays'] = run_query(client, query_holidays)

    # ura_season_carpark_list
    return fetched_data

def preprocess_data(data_dict: dict):
    """Preprocesses the  data."""

    feature_matrix = create_feature_matrix(data_dict)

    if feature_matrix.empty:
            raise ValueError("Feature matrix is empty. Cannot train.")

    X = feature_matrix.drop(columns="utilisation_rate")
    y = feature_matrix["utilisation_rate"]

    return {"X": X, "y": y}

def get_update_time_range(test_logical_date: pendulum.DateTime):
    """For initial testing, take 3 days ago to today as test data."""
    logical_date = test_logical_date

    end_date = logical_date.subtract(microseconds=1)
    start_date = logical_date.subtract(days=3) 

    # buffer window for weather / events / traffic
    buffer_start = start_date.subtract(hours=8)
    buffer_end = end_date.add(hours=1)

    return {
        "data_start_ts": start_date.to_iso8601_string(),
        "data_end_ts": end_date.to_iso8601_string(),
        "buffer_start_ts": buffer_start.to_iso8601_string(),
        "buffer_end_ts": buffer_end.to_iso8601_string(),
    }

def query_weekly_data_from_bq(time_range: dict):
    """Queries BigQuery for data within the specified time range."""
    try:
        client, _ = setup_bigquery_client(KEY_PATH)
    except Exception as e:
         raise 

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
    try:
        fetched_data['availability'] = run_query(client, query_avail)
    except Exception as e:
        raise

   
    query_weather = f"""
        SELECT station_id, latitude, longitude, datetime, rainfall_amount
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLES['weather']}`
        WHERE datetime >= TIMESTAMP("{buffer_start}") AND datetime < TIMESTAMP("{buffer_end}")
    """
    try:
        fetched_data['weather'] = run_query(client, query_weather)
    except Exception as e:
        fetched_data['weather'] = pd.DataFrame()

    query_events = f"""
        SELECT *
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLES['events']}`
    """
    try:
        fetched_data['events'] = run_query(client, query_events)
        fetched_data['events']['datetime'] = pd.to_datetime(fetched_data['events']['datetime']).dt.tz_localize('Asia/Singapore')
        # filter 
        fetched_data['events'] = fetched_data['events'][(
            fetched_data['events']['datetime'] >= pd.to_datetime(buffer_start).tz_convert('Asia/Singapore') ) &
            (fetched_data['events']['datetime'] < pd.to_datetime(buffer_end).tz_convert("Asia/Singapore")
        )]
    except Exception as e:
        fetched_data['events'] = pd.DataFrame()

    query_traffic = f"""
        SELECT latitude, longitude,  incident_time
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLES['traffic']}`
        WHERE incident_time >= TIMESTAMP("{buffer_start}") AND incident_time < TIMESTAMP("{buffer_end}")
    """
    try:
        fetched_data['traffic'] = run_query(client, query_traffic)
    except Exception as e:
        fetched_data['traffic'] = pd.DataFrame()

    query_list = f"""
        SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{TABLES['list']}` WHERE vehCat = "Car"
    """
    try:
        fetched_data['list'] = run_query(client, query_list)
    except Exception as e:
        fetched_data['list'] = pd.DataFrame()

    query_holidays = f"""
        SELECT date FROM `{PROJECT_ID}.{DATASET_ID}.{TABLES['holidays']}`
    """
    try:
        fetched_data['holidays'] = run_query(client, query_holidays)
    except Exception as e:
        fetched_data['holidays'] = pd.DataFrame()

    return fetched_data

def main():
    # get train data
    raw_data = query_all_historical_data()
    processed_training_data = preprocess_data(raw_data)
    X_train = processed_training_data["X"]
    y_train = processed_training_data["y"]

    pipeline = Pipeline([
        ('scaler', StandardScaler()),
        ('sgd_regressor', SGDRegressor(max_iter=1000, tol=1e-3, random_state=42))
    ])
    pipeline.fit(X_train, y_train)

    # get test data
    test_run_date = pendulum.datetime(2025, 4, 25, tz="Asia/Singapore")
    time_range_dict = get_update_time_range(test_run_date)
    raw_test_data = query_weekly_data_from_bq(time_range_dict)
    processed_test_data = preprocess_data(raw_test_data)

    X_test, y_test = processed_test_data["X"], processed_test_data["y"]
    y_pred = pipeline.predict(X_test)
    
    mse = mean_squared_error(y_test, y_pred)
    mae = mean_absolute_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)
    
    metrics = {
        "mean_squared_error": mse,
        "mean_absolute_error": mae,
        "r_squared": r2,
        "sample_count": len(X_test)
    }

    print(metrics)

    # print("=== SAVING MODEL ===")
    # os.makedirs(os.path.dirname(LOCAL_MODEL_PATH), exist_ok=True)
    # joblib.dump(pipeline, LOCAL_MODEL_PATH)
    # print("=== MODEL SAVED ===")
if __name__ == '__main__':
    main()