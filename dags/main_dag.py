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
from modules.scrape_all_data import transform_ura_data,transform_events_data, transform_traffic_incidents, transform_sg_public_holidays
from modules.bigquery_utils import setup_bigquery_client, create_dataset_if_not_exists, upload_dataframe_to_bigquery,verify_upload

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10)
}


@dag(dag_id='carpark_etl_taskflow', default_args=default_args, schedule="@daily", catchup=False, tags=['project'])
def carpark_taskflow_api_etl():
    @task
    def get_project_id(key_path: str):
        #client, project_id = setup_bigquery_client(key_path)
        #info_dict= {
        #    "client": client,
        #    "project_id": project_id
        #}
        #return info_dict
        _, project_id = setup_bigquery_client(key_path)
        return project_id
    
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


        #df_holiday.to_csv("holiday.csv",index=False)

        all_data = {
            "ura_availability" : df_ura_avail,
            "ura_carpark_list": df_ura_carpark_list,
            "ura_season_carpark": df_ura_season_carpark,
            "events": df_events,
            "traffic": df_traffic,
            "public_holiday": df_holiday
        }
        return all_data
    

    @task
    def bigquery_upload(all_data: dict, project_id: str, key_path: str):
        client, _ = setup_bigquery_client(key_path)
        #project_id=info_dict['project_id'] #client: str, project_id: str
        #dataset_id = "is3107-457309.singapore_datasets"
        dataset_id= "singapore_datasets"
        table_ids = {
            "ura_availability": "ura_carpark_availability",
            "ura_carpark_list": "ura_carpark_list",
            "ura_season_carpark": "ura_season_carpark_list",
            "events": "events",
            "traffic": "traffic_incidents",
            "public_holiday": "public_holidays"
        }

        key_list=["ura_availability", "ura_carpark_list","ura_season_carpark","events","traffic","public_holiday"]

        for i in range(len(key_list)):
            upload_dataframe_to_bigquery(client,all_data[key_list[i]],project_id,dataset_id,table_ids[key_list[i]])
            verify_upload(client,project_id,dataset_id,table_ids[key_list[i]])
        return "done"

    
    key_path = "/keys/is3107-457309-0e9066063708.json"
    #info_dict= init_bigquery_client(key_path)
    project_id=get_project_id(key_path)
    all_data=extract_all_data()
    bigquery_upload(all_data,project_id,key_path)





project_etl_dag = carpark_taskflow_api_etl()