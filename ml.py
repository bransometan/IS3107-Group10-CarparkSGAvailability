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

# Configuration
KEY_PATH = './key/is3107-457309-0e9066063708.json'
DATASET_ID = 'singapore_datasets'
LOCAL_MODEL_PATH = "./model/sgd_regressor_pipeline.joblib"

# --- Utility functions ---

def parse_time_string(time_str):
    """
    Parses a time string in 'HH.MM AM/PM' format into a datetime.time object.
    Handles potential errors and NaNs.
    """
    if pd.isna(time_str) or not isinstance(time_str, str):
        return pd.NaT # Return Not a Time for missing or non-string input
    try:
        return pd.to_datetime(time_str, format='%I.%M %p').time()
    except ValueError:
        print(f"Warning: Could not parse time string: {time_str}")
        return pd.NaT

def parse_rate_duration(duration_str):
    """
    Parse weekdayMin or satdayMin to get the rate duration.
    """
    if pd.isna(duration_str) or not isinstance(duration_str, str):
        return np.nan 
    try:
        return float(duration_str.split()[0].strip())
    except ValueError:
        # Handle cases where format might be slightly off or invalid
        print(f"Warning: Could not parse duration string: {duration_str}")
        return np.nan
    
def convert_rate_per_hr(rate, rate_duration):
    """
    Converts given rate to rate per hour

    Args:
        rate (float): rate per given rate_duration
        rate_duration (float): rate duration given, in minutes
    """
    return rate * rate_duration / 60

def find_parking_rate(availability_row, ura_carpark_list):
    """
    Finds the appropriate parking rate for a given availability record.

    Args:
        availability_row (pd.Series): A row from the ura_carpark_availability.
        ura_carpark_list (pd.DataFrame)

    Returns:
        float: The applicable parking rate, or np.nan if not found or data is missing.
    """
    carpark_no = availability_row['carparkNo']
    check_datetime = availability_row['datetime']
    check_time = check_datetime.time()
    is_weekday = check_datetime.dayofweek < 5

    # Filter the rates dataframe for the specific carpark
    possible_rates = ura_carpark_list[ura_carpark_list['carparkNo'] == carpark_no]

    if possible_rates.empty:
        return np.nan

    # Iterate through the time slots for this carpark
    for _, rate_row in possible_rates.iterrows():
        start_time = rate_row['start_time_obj']
        end_time = rate_row['end_time_obj']

        # Skip if time objects are invalid (NaT)
        if pd.isna(start_time) or pd.isna(end_time):
            continue

        # Check if the check_time falls within the rate's time interval
        time_match = False
        if start_time <= end_time:
            if start_time <= check_time < end_time:
                time_match = True
     
        else: # Overnight interval (e.g., 22:00 to 06:00)
            # Check if time is >= start_time OR < end_time
            if check_time >= start_time or check_time < end_time:
                time_match = True

        if not time_match:
            continue
        
        if is_weekday:
            rate_value = rate_row['weekdayRate']
            duration_str = rate_row['weekdayMin'] # Get duration string from rate_row
        else: # Weekend/PH
            rate_value = rate_row['satdayRate'] # Assume same weekend rate (Sat = Sun = PH)
            duration_str = rate_row['satdayMin'] # Get duration string from rate_row

        # Check if the selected rate value is valid
        if pd.isna(rate_value):
             continue # Skip to the next rate row if rate is missing for this slot

        rate_duration_minutes = parse_rate_duration(duration_str)

        # Check if the duration is valid
        if pd.isna(rate_duration_minutes):
           continue # Skip to the next rate row if duration is missing/invalid

        hourly_rate = convert_rate_per_hr(rate_value, rate_duration_minutes)

        # Check if the final hourly rate is valid
        if pd.isna(hourly_rate):
             continue # Skip to the next rate row if conversion failed

        return hourly_rate

    # If no matching time slot was found for this carpark
    # print(f"Debug: No matching time slot found for {carpark_no} at {check_time}")
    return np.nan

def add_parking_rates(df_avail: pd.DataFrame, df_list: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a 'parking_rate' column to the availability DataFrame based on
    rates defined in the list DataFrame.

    Args:
        df_avail: DataFrame like ura_carpark_availability. Requires 'carparkNo', 'datetime'.
        df_list: DataFrame like ura_carpark_list. Requires 'carparkNo', 'startTime',
                 'endTime', 'weekdayRate', 'satdayRate'.

    Returns:
        Original df_avail DataFrame with an added 'parking_rate' column.
    """
    # Make a copy to avoid modifying the original df_list outside the function
    df_list_processed = df_list.copy()
    # Parse time strings into time objects
    df_list_processed['start_time_obj'] = df_list_processed['startTime'].apply(parse_time_string)
    df_list_processed['end_time_obj'] = df_list_processed['endTime'].apply(parse_time_string)

    # Handle potential NaNs in critical rate columns if necessary (optional)
    df_list_processed['weekdayRate'] = df_list_processed['weekdayRate'].fillna(value=np.nan)
    df_list_processed['satdayRate'] = df_list_processed['satdayRate'].fillna(value=np.nan)

    # Apply the lookup function row by row
    df_avail['parking_rate'] = df_avail.apply(
        lambda row: find_parking_rate(row, df_list_processed),
        axis=1
    )
    return df_avail

def get_nearby_rainfall(carpark_row, stations_df, weather_df, radius_km, window):
    """
    Calculates the mean rainfall from nearby weather stations within a time window.

    Args:
        carpark_row (pd.Series): A row from the carpark availability DataFrame.
        stations_df (pd.DataFrame): DataFrame with unique station locations ('station_id', 'latitude', 'longitude').
        weather_df (pd.DataFrame): The full weather rainfall DataFrame.
        radius_km (float): The radius within which to search for stations.
        window (timedelta): Time window to consider

    Returns:
        float: The mean rainfall amount, or np.nan if no data is found.
    """
    carpark_loc = (carpark_row['latitude'], carpark_row['longitude'])
    carpark_time = carpark_row['datetime'].tz_localize('Asia/Singapore')

    nearby_station_ids = []
    for idx, station_row in stations_df.iterrows():
        station_loc = (station_row['latitude'], station_row['longitude'])
        distance = geodesic(carpark_loc, station_loc).km
        if distance <= radius_km:
            nearby_station_ids.append(station_row['station_id'])

    # Handle case where no stations are nearby
    if not nearby_station_ids:
        # print(f"No stations within {radius_km}km of {carpark_loc} for time {carpark_time}")
        return 0

    # Define time window (+/- 1 hour)
    start_time = carpark_time - window
    end_time = carpark_time + window

    # Filter weather data for nearby stations AND within the time window
    relevant_weather = weather_df[
        (weather_df['station_id'].isin(nearby_station_ids)) &
        (weather_df['datetime'] >= start_time) &
        (weather_df['datetime'] <= end_time)
    ]

    # Handle case where no rainfall data is found in the time window for nearby stations
    if relevant_weather.empty:
       # print(f"No rainfall data found for stations {nearby_station_ids} between {start_time} and {end_time}")
       return 0

    # Calculate and return the mean rainfall
    mean_rainfall = relevant_weather['rainfall_amount'].mean()
    return mean_rainfall

def check_nearby_event(carpark_row, all_events_df, radius_km, time_window):
    """
    Checks if any event occurred nearby within a specified time window *before* the carpark timestamp.

    Args:
        carpark_row (pd.Series): A row from the carpark availability DataFrame.
        all_events_df (pd.DataFrame): DataFrame containing all events.
        radius_km (float): The radius (in km) to define 'nearby'.
        time_window (pd.Timedelta): The lookback period (e.g., 8 hours).

    Returns:
        int: 1 if a nearby event is found within the time window, 0 otherwise.
    """
    target_tz = 'Asia/Singapore'
    carpark_loc = (carpark_row['latitude'], carpark_row['longitude'])
    carpark_time = carpark_row['datetime'].tz_localize(target_tz)

    # Define the start and end of the time window (looking backwards)
    start_time = carpark_time - time_window
    end_time = carpark_time # Events must happen *before* the carpark timestamp

    # Optimization: Filter events by time first
    events_in_window = all_events_df[
        (all_events_df['datetime'] >= start_time) &
        (all_events_df['datetime'] < end_time) # Use '<' as event must be before
    ]

    # If no events happened in the time window at all, we can stop
    if events_in_window.empty:
        return 0

    # Now check the spatially filtered events for proximity
    for idx, event_row in events_in_window.iterrows():
        event_loc = (event_row['latitude'], event_row['longitude'])
        distance = geodesic(carpark_loc, event_loc).km
        if distance <= radius_km:
            return 1 # Found a nearby event in the time window

    # If loop completes without finding a nearby event
    return 0

def check_nearby_traffic(carpark_row, traffic_df, radius_km, time_window):
    """
    Checks if any traffic incident occurred nearby within a specified time window *before* the carpark timestamp.

    Args:
        carpark_row (pd.Series): A row from the carpark availability DataFrame.
        traffic_df (pd.DataFrame): DataFrame containing all traffic incidents.
        radius_km (float): The radius (in km) to define 'nearby'.
        time_window (pd.Timedelta): The lookback period (e.g., 8 hours).

    Returns:
        int: 1 if a nearby event is found within the time window, 0 otherwise.
    """
    target_tz = 'Asia/Singapore'
    carpark_loc = (carpark_row['latitude'], carpark_row['longitude'])
    carpark_time = carpark_row['datetime'].tz_localize(target_tz)

    # Define the start and end of the time window (looking backwards)
    start_time = carpark_time - time_window
    end_time = carpark_time # Events must happen *before* the carpark timestamp

    # Optimization: Filter events by time first
    traffic_incidents_in_window = traffic_df[
        (traffic_df['incident_time'] >= start_time) &
        (traffic_df['incident_time'] < end_time) # Use '<' as event must be before
    ]

    # If no events happened in the time window at all, we can stop
    if traffic_incidents_in_window.empty:
        return 0

    # Now check the spatially filtered events for proximity
    for idx, event_row in traffic_incidents_in_window.iterrows():
        event_loc = (event_row['latitude'], event_row['longitude'])
        distance = geodesic(carpark_loc, event_loc).km
        if distance <= radius_km:
            return 1 # Found a nearby event in the time window

    # If loop completes without finding a nearby event
    return 0

def create_feature_matrix(data_dict):
    """
    Prepare data for machine learning training.
    """
    ura_carpark_availability = data_dict['ura_carpark_availability'].dropna()
    ura_carpark_list = data_dict['ura_carpark_list'].dropna()
    ura_season_carpark_list = data_dict['ura_season_carpark_list'].dropna()
    weather_rainfall = data_dict['weather_rainfall'].dropna()
    events = data_dict['events'].dropna()
    traffic_incidents = data_dict['traffic_incidents'].dropna()
    public_holidays = data_dict['public_holidays'].dropna()

    # extract dates/times
    public_holidays_dates = set(public_holidays["date"].dt.date)
    # incident_times = pd.to_datetime(traffic_incidents['incident_time'], errors='coerce').dropna()
    
    # extract unique weather stations
    unique_stations = weather_rainfall[['station_id', 'latitude', 'longitude']].drop_duplicates().reset_index(drop=True)
    
    # extract unique carparks (to get capacity)
    unique_carparks = ura_carpark_list.drop_duplicates(subset='carparkNo', keep='first')
    capacity_map = unique_carparks.set_index('carparkNo')['parkCapacity']

    # build feature matrix    
    df = ura_carpark_availability.copy()
    df["total_lots"] = df["carparkNo"].map(capacity_map)
    df["hour_of_day"] = df["time"].apply(lambda x: x.hour)
    df["day_of_week"] = df["datetime"].apply(lambda x: x.dayofweek)
    df["is_weekend"] = df["day_of_week"].apply(lambda x: x > 4)
    df["is_holiday"] = df["date"].apply(lambda x: x in public_holidays_dates)
    
    # check traffic within +/-3 hr, within 2.0 km radius of the carpark
    radius_km = 2.0
    window = pd.Timedelta(hours=3)
    df['is_traffic'] = df.apply(
        check_nearby_traffic,
        axis=1, 
        args=(traffic_incidents, radius_km, window) # Pass additional arguments
    )

    # check rainfall within +/-1 hr, within 2.0 km radius of the carpark
    radius_km = 2.0
    window = pd.Timedelta(hours=1)
    df['rainfall'] = df.apply(
        get_nearby_rainfall,
        axis=1, 
        args=(unique_stations, weather_rainfall, radius_km, window) # Pass additional arguments
    )

    # check events within -8 hrs, within 2.0 km radius
    radius_km = 2.0
    window = pd.Timedelta(hours=8)
    df['has_nearby_event'] = df.apply(
        check_nearby_event,
        axis=1,
        args=(events, radius_km, window)
    )

    # season rate
    # get season rate here; to be implemented?

    # add rates
    df = add_parking_rates(df, ura_carpark_list)

    utilisation_rate = 1 - df["lotsAvailable"] / df["total_lots"]
    df["utilisation_rate"] = utilisation_rate.clip(lower=0, upper=1)

    return df[["hour_of_day", "day_of_week", "is_weekend",
               "is_holiday", "rainfall", "is_traffic", "has_nearby_event",
               "total_lots", "parking_rate", "utilisation_rate"]]

def initial_train_and_save(X_train: pd.DataFrame, y_train: pd.Series,
                           local_model_path: str = LOCAL_MODEL_PATH):
    """
    Trains an initial SGDRegressor model using a pipeline with StandardScaler,
    saves it locally, and uploads it to Google Cloud Storage.

    Args:
        X_train: DataFrame or NumPy array of training features.
        y_train: Series or NumPy array of the training target variable.
        gcs_bucket_name: The name of the GCS bucket to upload the model to.
        model_gcs_path: The path (key) within the GCS bucket to save the model.
        local_model_path: Temporary local path to save the model before upload.
    """
   
    pipeline = Pipeline([
        ('scaler', StandardScaler()),
        ('sgd_regressor', SGDRegressor(max_iter=1000, tol=1e-3, random_state=42))
    ])
    pipeline.fit(X_train, y_train)

    # Ensure the directory exists
    print("=== SAVING MODEL ===")
    os.makedirs(os.path.dirname(local_model_path), exist_ok=True)
    joblib.dump(pipeline, local_model_path)
    print("=== MODEL SAVED ===")

def main():
    data_dict = fetch_all_data() # I will update this to query from bigquery
    feature_matrix = create_feature_matrix(data_dict)
    X_train, y_train = feature_matrix.drop(columns="utilisation_rate"), feature_matrix["utilisation_rate"]
    initial_train_and_save(X_train, y_train, LOCAL_MODEL_PATH)

if __name__ == '__main__':
    main()