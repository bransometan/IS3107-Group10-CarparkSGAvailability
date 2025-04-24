from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

import joblib
import numpy as np
import pandas as pd
from geopy.distance import geodesic

# Sklearn imports
from sklearn.linear_model import SGDRegressor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

LOCAL_MODEL_DIR = "./model" 
LOCAL_MODEL_FILENAME = "sgd_regressor_pipeline.joblib"
LOCAL_MODEL_PATH = os.path.join(LOCAL_MODEL_DIR, LOCAL_MODEL_FILENAME)

# --- Feature Engineering ---
def parse_time_string(time_str):
    if pd.isna(time_str) or not isinstance(time_str, str):
        return pd.NaT
    try:
        # handle variations '12.00 AM' vs '12:00 AM'
        time_str_corr = time_str.replace('.', ':')
        return pd.to_datetime(time_str_corr, format='%I:%M %p', errors='coerce').time()
    except ValueError:
        logging.warning(f"Could not parse time string: {time_str}")
        return pd.NaT

def parse_rate_duration(duration_str):
    if pd.isna(duration_str) or not isinstance(duration_str, str):
        return np.nan
    try:
        # extract number
        parts = duration_str.split()
        if parts:
            return float(parts[0].strip())
        else:
            return np.nan
    except ValueError:
        logging.warning(f"Could not parse duration string: {duration_str}")
        return np.nan

def convert_rate_per_hr(rate, rate_duration):
    if pd.isna(rate) or pd.isna(rate_duration) or rate_duration == 0:
        return np.nan
    #  rate_duration is in minutes
    try:
        return float(rate) * (60.0 / float(rate_duration))
    except (ValueError, TypeError):
        logging.warning(f"Could not convert rate: rate={rate}, duration={rate_duration}")
        return np.nan

def find_parking_rate(availability_row, ura_carpark_list_processed):
    carpark_no = availability_row['carparkNo']
    check_datetime = availability_row['datetime'] 
    check_time = check_datetime.time()

    if check_datetime.tzinfo is None:
        check_datetime = check_datetime.tz_localize('Asia/Singapore') 
    is_weekday = check_datetime.dayofweek < 5

    possible_rates = ura_carpark_list_processed[ura_carpark_list_processed['carparkNo'] == str(carpark_no)]

    if possible_rates.empty:
        return np.nan

    applicable_rate = np.nan 

    for _, rate_row in possible_rates.iterrows():
        start_time = rate_row['start_time_obj']
        end_time = rate_row['end_time_obj']

        if pd.isna(start_time) or pd.isna(end_time):
            continue

        time_match = False
        if start_time <= end_time: # Normal interval (e.g., 07:00 to 19:00)
            if start_time <= check_time < end_time:
                 time_match = True
        else: # Overnight interval (e.g., 22:00 to 06:00)
            if check_time >= start_time or check_time < end_time:
                 time_match = True

        if not time_match:
            continue

        # determine rate based on day
        if is_weekday:
            rate_value = rate_row['weekdayRate']
            duration_str = rate_row['weekdayMin']
        else: # Assume Sat/Sun/PH use the same rate structure provided
            rate_value = rate_row['satdayRate']
            duration_str = rate_row['satdayMin']

        # remove '$' if present
        if isinstance(rate_value, str):
            rate_value = rate_value.replace('$', '').strip()
        try:
            rate_value = float(rate_value)
        except (ValueError, TypeError):
            continue # skip if rate is not a valid number

        if pd.isna(rate_value):
            continue

        rate_duration_minutes = parse_rate_duration(duration_str)

        if pd.isna(rate_duration_minutes):
            continue 

        hourly_rate = convert_rate_per_hr(rate_value, rate_duration_minutes)

        if not pd.isna(hourly_rate):
            applicable_rate = hourly_rate
            break

    return applicable_rate

def add_parking_rates(df_avail, df_list) -> pd.DataFrame:
    if df_avail.empty or df_list.empty:
        df_avail['parking_rate'] = np.nan
        return df_avail

    df_list_processed = df_list.copy()
    df_list_processed['start_time_obj'] = df_list_processed['startTime'].apply(parse_time_string)
    df_list_processed['end_time_obj'] = df_list_processed['endTime'].apply(parse_time_string)

    df_list_processed['carparkNo'] = df_list_processed['carparkNo'].astype(str)
    df_avail['carparkNo'] = df_avail['carparkNo'].astype(str)

    df_list_processed['weekdayRate'] = pd.to_numeric(
        df_list_processed['weekdayRate'].astype(str).str.replace('$', '', regex=False), errors='coerce'
    )
    df_list_processed['satdayRate'] = pd.to_numeric(
        df_list_processed['satdayRate'].astype(str).str.replace('$', '', regex=False), errors='coerce'
    )

    df_avail['parking_rate'] = df_avail.apply(
        lambda row: find_parking_rate(row, df_list_processed),
        axis=1
    )
    null_rates = df_avail['parking_rate'].isnull().sum()
    if null_rates > 0:
        logging.warning(f"{null_rates} out of {len(df_avail)} records have null parking rates.")
    return df_avail

def get_nearby_rainfall(carpark_row, stations_df, weather_df, radius_km, window_td):
    # Ensure required columns exist
    if not all(col in carpark_row.index for col in ['latitude', 'longitude', 'datetime']):
        logging.warning("Missing required columns in carpark_row for rainfall calculation.")
        return 0.0 
    if stations_df.empty or not all(col in stations_df.columns for col in ['station_id', 'latitude', 'longitude']):
        logging.warning("Stations DataFrame is empty or missing columns.")
        return 0.0
    if weather_df.empty or not all(col in weather_df.columns for col in ['station_id', 'datetime', 'rainfall_amount']):
        logging.debug("Weather DataFrame is empty or missing columns.")
        return 0.0

    carpark_loc = (carpark_row['latitude'], carpark_row['longitude'])
    carpark_time = carpark_row['datetime'] 

    nearby_station_ids = []
    for idx, station_row in stations_df.iterrows():
        station_loc = (station_row['latitude'], station_row['longitude'])
        try:
            distance = geodesic(carpark_loc, station_loc).km
            if distance <= radius_km:
                nearby_station_ids.append(station_row['station_id'])
        except ValueError:
            logging.warning(f"Could not calculate distance for station {station_row.get('station_id', 'N/A')}")
            continue

    if not nearby_station_ids:
        return 0.0

    # Define time window
    start_time = carpark_time - window_td
    end_time = carpark_time + window_td

    # Filter weather data
    relevant_weather = weather_df[
        (weather_df['station_id'].isin(nearby_station_ids)) &
        (weather_df['datetime'] >= start_time) &
        (weather_df['datetime'] <= end_time)
    ]

    if relevant_weather.empty:
        return 0.0

    mean_rainfall = relevant_weather['rainfall_amount'].mean(skipna=True)
    return mean_rainfall if not pd.isna(mean_rainfall) else 0.0

def check_nearby_event(carpark_row, all_events_df, radius_km, time_window_td):
    if not all(col in carpark_row.index for col in ['latitude', 'longitude', 'datetime']):
        logging.warning("Missing required columns in carpark_row for event check.")
        return 0
    if all_events_df.empty or not all(col in all_events_df.columns for col in ['latitude', 'longitude', 'datetime']):
        logging.debug("Events DataFrame empty or missing columns.")
        return 0

    carpark_loc = (carpark_row['latitude'], carpark_row['longitude'])
    carpark_time = carpark_row['datetime'] # Assume timezone-aware

    start_time = carpark_time - time_window_td
    end_time = carpark_time 

    # Filter events by time first
    events_in_window = all_events_df[
        (all_events_df['datetime'] >= start_time) &
        (all_events_df['datetime'] <= end_time) 
    ]

    if events_in_window.empty:
        return 0

    # Check proximity
    for idx, event_row in events_in_window.iterrows():
        event_loc = (event_row['latitude'], event_row['longitude'])
        try:
            distance = geodesic(carpark_loc, event_loc).km
            if distance <= radius_km:
                return 1
        except ValueError:
            logging.warning(f"Could not calculate distance for event at {event_loc}")
            continue

    return 0

def check_nearby_traffic(carpark_row, traffic_df, radius_km, time_window_td):

    if not all(col in carpark_row.index for col in ['latitude', 'longitude', 'datetime']):
        logging.warning("Missing required columns in carpark_row for traffic check.")
        return 0
    if traffic_df.empty or not all(col in traffic_df.columns for col in ['latitude', 'longitude', 'incident_time']):
        logging.debug("Traffic DataFrame empty or missing columns.")
        return 0

    carpark_loc = (carpark_row['latitude'], carpark_row['longitude'])
    carpark_time = carpark_row['datetime'] # Assume timezone-aware

    # Define time window (lookback from carpark time)
    start_time = carpark_time - time_window_td
    end_time = carpark_time

    # Filter traffic incidents by time
    incidents_in_window = traffic_df[
        (traffic_df['incident_time'] >= start_time) &
        (traffic_df['incident_time'] <= end_time)
    ]

    if incidents_in_window.empty:
        return 0

    # Check proximity
    for idx, incident_row in incidents_in_window.iterrows():
        incident_loc = (incident_row['latitude'], incident_row['longitude'])
        try:
            distance = geodesic(carpark_loc, incident_loc).km
            if distance <= radius_km:
                return 1
        except ValueError:
            logging.warning(f"Could not calculate distance for traffic incident at {incident_loc}")
            continue

    return 0

def create_feature_matrix(data_dict):
    """
    Prepares the feature matrix for prediction or partial fitting.
    Expects data_dict keys to match the TABLES constant values.
    """

    # validate tables
    required_tables = ['availability', 'list', 'weather', 'events', 'traffic', 'holidays']
    if not all(table in data_dict for table in required_tables):
        raise ValueError(f"Missing required tables in data_dict. Need: {required_tables}")

    ura_carpark_availability = data_dict['availability'].copy()
    ura_carpark_list = data_dict['list'].copy()
    weather_rainfall = data_dict['weather'].dropna()
    events = data_dict['events'].dropna()
    traffic_incidents = data_dict['traffic'].dropna()
    public_holidays = data_dict['holidays'].dropna()

    # handle empty tablees
    if ura_carpark_availability.empty:
        logging.warning("URA carpark availability data is empty. Cannot create features.")
        # return empty df with expected columns for downstream tasks
        return pd.DataFrame(columns=[
            "hour_of_day", "day_of_week", "is_weekend", "is_holiday",
            "rainfall", "is_traffic", "has_nearby_event", "total_lots",
            "parking_rate", "utilisation_rate"
        ])

    # Type conversions and timezone handling ---
    logging.info("Converting data types and handling timezones...")
    try:
        target_tz = 'Asia/Singapore'
        for df_name, col_name in [('availability', 'datetime'), ('weather', 'datetime'),
                                  ('events', 'datetime'), ('traffic', 'incident_time'),
                                  ('holidays', 'date')]:
            df = data_dict[df_name] # work on the original dict temporarily for lookup
            if col_name in df.columns:
                df[col_name] = pd.to_datetime(df[col_name], errors='coerce')
                df.dropna(subset=[col_name], inplace=True)
                # localize or convert timezone
                if df[col_name].dt.tz is None:
                    df[col_name] = df[col_name].dt.tz_localize(target_tz)
                else:
                    df[col_name] = df[col_name].dt.tz_convert(target_tz)

        # refresh local copies after timezone conversion
        ura_carpark_availability = data_dict['availability'].copy()
        weather_rainfall = data_dict['weather'].copy()
        events = data_dict['events'].copy()
        traffic_incidents = data_dict['traffic'].copy()
        public_holidays = data_dict['holidays'].copy()

        for df in [ura_carpark_availability, ura_carpark_list, weather_rainfall, events, traffic_incidents]:
            df.dropna(subset=['latitude', 'longitude'], inplace=True) 

        ura_carpark_availability['lotsAvailable'] = pd.to_numeric(ura_carpark_availability['lotsAvailable'], errors='coerce')
        ura_carpark_availability.dropna(subset=['lotsAvailable'], inplace=True)
        ura_carpark_availability['lotsAvailable'] = ura_carpark_availability['lotsAvailable'].astype(int)

        # Pre-parse holidays for faster lookup
        public_holidays_dates = set(public_holidays["date"].dt.date) if not public_holidays.empty else set()

        # Prepare unique station locations
        unique_stations = weather_rainfall[['station_id', 'latitude', 'longitude']].drop_duplicates().reset_index(drop=True) if not weather_rainfall.empty else pd.DataFrame()

        # Prepare carpark capacity map (handle potential duplicates, take first)
        unique_carparks = ura_carpark_list.drop_duplicates(subset='carparkNo', keep='first')
        unique_carparks.loc[:, 'parkCapacity'] = pd.to_numeric(unique_carparks['parkCapacity'], errors='coerce')
        
        # unique_carparks['parkCapacity'].fillna(0, inplace=True)
        capacity_map = unique_carparks.set_index('carparkNo')['parkCapacity']


    except Exception as e:
        logging.error(f"Error during data preparation: {e}", exc_info=True)
        raise

    # Feature eng
    df = ura_carpark_availability.copy()

    # total lots
    df["total_lots"] = df["carparkNo"].map(capacity_map)
    df.dropna(subset=['total_lots'], inplace=True) # Remove rows where capacity is unknown
    df['total_lots'] = df['total_lots'].astype(int) # Convert to int after dropna

    # datetime is timezone-aware column
    df["hour_of_day"] = df["datetime"].dt.hour
    df["day_of_week"] = df["datetime"].dt.dayofweek # Monday=0, Sunday=6
    df["is_weekend"] = df["day_of_week"].isin([5, 6]).astype(bool) # Saturday=5, Sunday=6
    df["is_holiday"] = df["datetime"].dt.date.isin(public_holidays_dates).astype(bool)

    # traffic
    radius_km_traffic = 2.0
    window_traffic = pd.Timedelta(hours=3) # look back 3 hours
    df['is_traffic'] = df.apply(
        check_nearby_traffic,
        axis=1,
        args=(traffic_incidents, radius_km_traffic, window_traffic)
    ).astype(bool)

    # nearby rainfall
    radius_km_rain = 2.0
    window_rain = pd.Timedelta(hours=1) # +/- 1 hour window
    df['rainfall'] = df.apply(
        get_nearby_rainfall,
        axis=1,
        args=(unique_stations, weather_rainfall, radius_km_rain, window_rain)
    )
    df['rainfall'] = df['rainfall'].fillna(0) 

    # nearby event
    radius_km_event = 2.0
    window_event = pd.Timedelta(hours=8) # Look back 8 hours
    df['has_nearby_event'] = df.apply(
        check_nearby_event,
        axis=1,
        args=(events, radius_km_event, window_event)
    ).astype(bool)

    # parking rate
    # impute missing
    df = add_parking_rates(df, ura_carpark_list)
    mean_rate = df['parking_rate'].mean()
    df['parking_rate'] = df['parking_rate'].fillna(mean_rate)

    # utilisation
    # Avoid division by zero or negative capacity
    valid_capacity_mask = df["total_lots"] > 0
    df["utilisation_rate"] = np.nan 
    df.loc[valid_capacity_mask, "utilisation_rate"] = 1.0 - (df.loc[valid_capacity_mask, "lotsAvailable"] / df.loc[valid_capacity_mask, "total_lots"])
    
    df["utilisation_rate"] = df["utilisation_rate"].clip(lower=0.0, upper=1.0)
    df.dropna(subset=['utilisation_rate'], inplace=True)

    logging.info("Selecting final features...")
    feature_cols = [
        "hour_of_day", "day_of_week", "is_weekend", "is_holiday",
        "rainfall", "is_traffic", "has_nearby_event",
        "total_lots", "parking_rate"
    ]
    target_col = "utilisation_rate"

    final_cols = feature_cols + [target_col]
    df_final = df[final_cols].copy()

    # Final check for NaNs in feature columns
    nan_check = df_final[feature_cols].isnull().sum()
    if nan_check.sum() > 0:
        df_final.dropna(subset=feature_cols, inplace=True)

    logging.info(f"Feature matrix created with {len(df_final)} rows and columns: {df_final.columns.tolist()}")

    if df_final.empty:
         logging.warning("Final feature matrix is empty after processing and NaN handling.")

    return df_final

def initial_train_and_save(X_train: pd.DataFrame, y_train: pd.Series,
                           local_model_path: str = LOCAL_MODEL_PATH):   
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