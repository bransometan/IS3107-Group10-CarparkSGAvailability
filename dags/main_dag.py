from airflow.decorators import dag, task
from datetime import datetime, timedelta
import requests
import pandas as pd
import numpy as np
import folium
import matplotlib.pyplot as plt
from pyproj import Transformer
from haversine import haversine, Unit
from tqdm import tqdm
from google.oauth2 import service_account
from google.cloud import bigquery
from modules.scrape_all_data import extract_ura_data,extract_events_data,extract_traffic_incidents,extract_sg_public_holidays,extract_weather_data
from modules.scrape_all_data import transform_ura_data,transform_events_data, transform_traffic_incidents, transform_sg_public_holidays, transform_weather_data
from modules.bigquery_utils import setup_bigquery_client, create_dataset_if_not_exists, upload_dataframe_to_bigquery,verify_upload
from modules.upload_all import upload_all_data_to_bigquery
from modules.dashboard_app import plot_carpark_utilization, plot_events_by_type,plot_rainfall_distribution,plot_top_carparks,plot_traffic_incidents_by_type
from modules.dashboard_app import get_top_carparks_by_availability, get_carpark_utilization, get_events_data, get_rainfall_data, get_traffic_incidents, run_query, main
import os

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=10)
}

SAVE_CHARTS = True
OUTPUT_DIR = 'reports'

# Create output directory if it doesn't exist
if SAVE_CHARTS and not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

@dag(dag_id='carpark_etl_taskflow', default_args=default_args, schedule="@daily", catchup=False, tags=['project'])
def carpark_taskflow_api_etl():
    
    @task
    def extract_all_data():
        #access keys and all (URA, events, traffic, public hol need)
        ura_access_key = "fb1d226d-6f7f-4ec4-b678-247dee275ca7"
        hol_dataset_id = ["d_3751791452397f1b1c80c451447e40b7",
                            "d_4e19214c3a5288eab7a27235f43da4fa",
                            "d_0b0fe4a7101674be0a359e55d32cd126"]
        events_api_key = "r4VgRmHwVhs5uG1WlqXzprvFWCSK8lyf"
        traffic_acc_key="brlj8fVmS+u/X9vpSjismQ=="


        #EXTRACTING DATA
        ura_data = extract_ura_data(ura_access_key)
        weather_data = extract_weather_data()
        events_data = extract_events_data(events_api_key)
        traffic_data = extract_traffic_incidents(traffic_acc_key)
        hol_data = extract_sg_public_holidays(hol_dataset_id)
        

        #TRANSFORMING DATA
        ura_data_transformed = transform_ura_data(ura_data)
        df_ura_avail = ura_data_transformed['availability']
        df_ura_carpark_list = ura_data_transformed['car_park_list']
        df_ura_season_carpark = ura_data_transformed['season_car_park_list']
        df_events=transform_events_data(events_data)
        df_traffic = transform_traffic_incidents(traffic_data)
        df_holiday = transform_sg_public_holidays(hol_data)
        df_weather=transform_weather_data(weather_data)


        #df_holiday.to_csv("holiday.csv",index=False)

        all_data = {
            "ura_carpark_availability" : df_ura_avail,
            "ura_carpark_list": df_ura_carpark_list,
            "ura_season_carpark_list": df_ura_season_carpark,
            "events": df_events,
            "traffic_incidents": df_traffic,
            "public_holidays": df_holiday,
            "weather_rainfall": df_weather

        }
        return all_data
    

    @task
    def bigquery_upload(all_data: dict, key_path: str):
        dataset_id= "singapore_datasets"
        upload_results=upload_all_data_to_bigquery(all_data,key_path)
        return upload_results


    @task 
    def visualisations():
        test= main()
        return test
    
    key_path = "/keys/is3107-457309-0e9066063708.json"
    all_data=extract_all_data()
    bigquery_upload(all_data,key_path)
    visualisations()





project_etl_dag = carpark_taskflow_api_etl()