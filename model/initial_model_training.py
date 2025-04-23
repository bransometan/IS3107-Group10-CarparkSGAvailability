import pandas as pd
import numpy as np
import datetime
from datetime import datetime
from geopy.distance import geodesic
import os
import joblib

from sklearn.linear_model import SGDRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from dags.modules.scrape_all_data import fetch_all_data
from dags.modules.bigquery_utils import run_query, setup_bigquery_client
from dags.modules.ml_utils import create_feature_matrix, initial_train_and_save

# Configuration
KEY_PATH = './key/is3107-457309-0e9066063708.json'
DATASET_ID = 'singapore_datasets'
LOCAL_MODEL_PATH = "./model/sgd_regressor_pipeline.joblib"

def main():
    data_dict = fetch_all_data() # I will update this to query from bigquery
    feature_matrix = create_feature_matrix(data_dict)
    print(feature_matrix)
    X_train, y_train = feature_matrix.drop(columns="utilisation_rate"), feature_matrix["utilisation_rate"]
    initial_train_and_save(X_train, y_train, LOCAL_MODEL_PATH)

if __name__ == '__main__':
    main()