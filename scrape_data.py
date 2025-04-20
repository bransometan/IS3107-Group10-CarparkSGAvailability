import requests
import pandas as pd
import pytz
import re
from datetime import datetime
from pyproj import Transformer

# ===== EXTRACTION FUNCTIONS =====

def extract_ura_data(access_key):
    """Extract car park data from URA API"""
    # Get URA Access Token
    token_url = "https://eservice.ura.gov.sg/uraDataService/insertNewToken/v1"
    headers_token = {
        "Cache-Control": "no-cache",
        "User-Agent": "PostmanRuntime/7.43.3",
        "Accept": "*/*",
        "Connection": "keep-alive",
        "AccessKey": access_key
    }

    token_response = requests.get(token_url, headers=headers_token)
    ura_access_token = token_response.json().get("Result")

    # Common headers for subsequent URA API calls
    common_headers = {
        "User-Agent": "PostmanRuntime/7.43.3",
        "Accept": "*/*",
        "Connection": "keep-alive",
        "AccessKey": access_key,
        "Token": ura_access_token
    }

    # Get Car Park Availability
    availability_url = "https://eservice.ura.gov.sg/uraDataService/invokeUraDS/v1?service=Car_Park_Availability"
    availability_response = requests.get(availability_url, headers=common_headers)
    availability_data = availability_response.json()

    # Get Car Park List and Rates
    car_park_list_url = "https://eservice.ura.gov.sg/uraDataService/invokeUraDS/v1?service=Car_Park_Details"
    car_park_list_response = requests.get(car_park_list_url, headers=common_headers)
    car_park_list_data = car_park_list_response.json()

    # Get Season Car Park List and Rates
    season_car_park_list_url = "https://eservice.ura.gov.sg/uraDataService/invokeUraDS/v1?service=Season_Car_Park_Details"
    season_car_park_list_response = requests.get(season_car_park_list_url, headers=common_headers)
    season_car_park_list_data = season_car_park_list_response.json()

    return {
        "availability": availability_data,
        "car_park_list": car_park_list_data,
        "season_car_park_list": season_car_park_list_data
    }

def extract_weather_data():
    """Extract rainfall data from data.gov.sg API"""
    rainfall_url = "https://api.data.gov.sg/v1/environment/rainfall"
    response = requests.get(rainfall_url)
    return response.json()

def extract_events_data(api_key):
    """Extract events data from STB API"""
    url = "https://api.stb.gov.sg/content/attractions/v2/search"
    headers = {
        "Content-Type": "application/json",
        "X-API-Key": api_key,
        "X-Content-Language": "en"
    }
    params = {
        "searchType": "keyword",
        "searchValues": "event"
    }
    response = requests.get(url, headers=headers, params=params)
    return response.json()

def extract_traffic_incidents(account_key):
    """Extract traffic incidents from LTA DataMall API"""
    url = "https://datamall2.mytransport.sg/ltaodataservice/TrafficIncidents"
    headers = {
        "AccountKey": account_key,
        "accept": "application/json"
    }
    response = requests.get(url, headers=headers)
    return response.json()

def extract_sg_public_holidays(dataset_ids):
    """Extract public holiday data from data.gov.sg API"""
    base_url = "https://data.gov.sg/api/action/datastore_search?resource_id="
    extracted_dfs = []

    for dataset_id in dataset_ids:
        url = base_url + dataset_id
        response = requests.get(url)
        data = response.json()
        records = data['result']['records']
        df = pd.DataFrame(records)
        extracted_dfs.append(df)

    return extracted_dfs

# ===== TRANSFORMATION FUNCTIONS =====

def transform_ura_data(ura_data):
    """Transform URA carpark data into structured dataframes"""
    # Create transformer: from SVY21 (EPSG:3414) to WGS84 (EPSG:4326)
    transformer = Transformer.from_crs("EPSG:3414", "EPSG:4326", always_xy=True)
    
    # Function to extract and convert from geometries column
    def convert_geom_to_latlong(geometry_list):
        try:
            if isinstance(geometry_list, list) and len(geometry_list) > 0:
                coords = geometry_list[0].get("coordinates")
                x, y = map(float, coords.split(","))
                lon, lat = transformer.transform(x, y)
                return pd.Series([lat, lon])
        except Exception:
            pass
        return pd.Series([None, None])
    
    # Create DataFrames
    df_availability = pd.DataFrame(ura_data["availability"]["Result"])
    df_carparklist = pd.DataFrame(ura_data["car_park_list"]["Result"])
    df_seasonlist = pd.DataFrame(ura_data["season_car_park_list"]["Result"])
    
    # Convert geometries to lat/long
    df_availability[["latitude", "longitude"]] = df_availability["geometries"].apply(convert_geom_to_latlong)
    df_carparklist[["latitude", "longitude"]] = df_carparklist["geometries"].apply(convert_geom_to_latlong)
    df_seasonlist[["latitude", "longitude"]] = df_seasonlist["geometries"].apply(convert_geom_to_latlong)
    
    # Clean up: drop original geom columns
    df_availability.drop(columns=["geometries"], inplace=True)
    df_carparklist.drop(columns=["geometries"], inplace=True)
    df_seasonlist.drop(columns=["geometries"], inplace=True)
    
    # Rename attributes
    df_carparklist.rename(columns={"ppCode": "carparkNo", "ppName": "carparkName"}, inplace=True)
    df_seasonlist.rename(columns={"ppCode": "carparkNo", "ppName": "carparkName"}, inplace=True)
    
    # Add datetime
    current_time_sg = pd.Timestamp.now(tz='Asia/Singapore').strftime("%Y-%m-%d %H:%M:%S")
    df_availability["datetime"] = current_time_sg
    df_availability['datetime'] = pd.to_datetime(df_availability['datetime'])
    
    # Create separate date and time columns
    df_availability["date"] = df_availability["datetime"].dt.date
    df_availability["time"] = df_availability["datetime"].dt.time
    
    # Convert data types
    df_availability["lotsAvailable"] = pd.to_numeric(df_availability["lotsAvailable"], errors='coerce')
    
    df_carparklist["weekdayRate"] = df_carparklist["weekdayRate"].str.replace("$", "").astype(str).str.strip().replace("", "0").astype(float)
    df_carparklist["satdayRate"] = df_carparklist["satdayRate"].str.replace("$", "").astype(str).str.strip().replace("", "0").astype(float)
    df_carparklist["sunPHRate"] = df_carparklist["sunPHRate"].str.replace("$", "").astype(str).str.strip().replace("", "0").astype(float)
    df_carparklist["parkCapacity"] = pd.to_numeric(df_carparklist["parkCapacity"], errors="coerce").fillna(0).astype(int)
    
    df_seasonlist["monthlyRate"] = pd.to_numeric(df_seasonlist["monthlyRate"], errors="coerce").fillna(0).astype(int)
    
    return {
        "availability": df_availability,
        "car_park_list": df_carparklist,
        "season_car_park_list": df_seasonlist
    }

def transform_weather_data(data):
    """Transform rainfall data into structured dataframe"""
    # Process the metadata for station info
    stations_df = pd.DataFrame(data['metadata']['stations'])
    
    # Extract latitude and longitude from the nested 'location' column
    stations_df['latitude'] = stations_df['location'].apply(lambda loc: loc['latitude'])
    stations_df['longitude'] = stations_df['location'].apply(lambda loc: loc['longitude'])
    
    # Keep only relevant columns and rename
    stations_df = stations_df[['id', 'name', 'latitude', 'longitude']].rename(columns={'id': 'station_id'})
    
    # Process the readings
    readings_item = data['items'][0]
    timestamp = readings_item['timestamp']
    readings_df = pd.DataFrame(readings_item['readings'])
    
    # Add timestamp and unit
    readings_df['timestamp'] = timestamp
    readings_df['unit'] = data['metadata']['reading_unit']
    
    # Merge the station and reading data
    result_df = pd.merge(stations_df, readings_df, on='station_id')
    
    # Convert timestamp to datetime in Singapore timezone and rename column
    result_df['datetime'] = pd.to_datetime(result_df['timestamp']).dt.tz_convert('Asia/Singapore')
    result_df.drop(columns=['timestamp'], inplace=True)
    
    # Rename 'value' to 'rainfall_amount'
    result_df.rename(columns={'value': 'rainfall_amount'}, inplace=True)
    
    # Reorder columns
    result_df = result_df[['station_id', 'name', 'latitude', 'longitude', 'datetime', 'unit', 'rainfall_amount']]
    
    # Sort by station ID
    return result_df.sort_values(by='station_id').reset_index(drop=True)

def transform_events_data(data):
    """Transform events data into structured dataframe"""
    # Get current Singapore time
    sg_time = datetime.now(pytz.timezone("Asia/Singapore"))
    
    # Clean & Flatten Data
    cleaned_data = []
    for item in data.get("data", []):
        location = item.get("location", {}) or {}
        address = item.get("address", {}) or {}
    
        cleaned_data.append({
            "name": item.get("name") or None,
            "type": item.get("type") or None,
            "description": item.get("description") or None,
            "latitude": location.get("latitude") or None,
            "longitude": location.get("longitude") or None,
            "street": address.get("streetName") or None,
            "postalCode": address.get("postalCode") or None,
            "rating": item.get("rating") or None,
            "nearestMRT": item.get("nearestMrtStation") or None,
            "admissionInfo": item.get("admissionInfo") or None,
            "ticketed": item.get("ticketed") or None,
            "officialWebsite": item.get("officialWebsite") or None,
            "datetime": sg_time  # keep as datetime object
        })
    
    # Convert to DataFrame
    df_events = pd.DataFrame(cleaned_data)
    
    # Fill missing values with <NA>
    df_events = df_events.fillna(value=pd.NA)
    
    # Convert to appropriate datatypes
    df_events = df_events.astype({
        "name": "object",
        "type": "object",
        "description": "object",
        "latitude": "float64",
        "longitude": "float64",
        "street": "object",
        "postalCode": "object",
        "rating": "float64",
        "nearestMRT": "object",
        "admissionInfo": "object",
        "ticketed": "category",
        "officialWebsite": "object"
    })
    
    # Ensure datetime is datetime64[ns]
    df_events["datetime"] = pd.to_datetime(df_events["datetime"])
    
    return df_events

def transform_traffic_incidents(data):
    """Transform traffic incidents data into structured dataframe"""
    # Convert to DataFrame
    df = pd.DataFrame(data["value"])
    
    # Parse 'Message' into 'incident_time' and 'incident_description'
    def split_message(msg):
        match = re.match(r"\((\d+/\d+)\)(\d{2}:\d{2}) (.+)", msg)
        if match:
            date_part = match.group(1)
            time_part = match.group(2)
            desc_part = match.group(3)
    
            # Convert to full datetime with current year and Singapore timezone
            sg_timezone = pytz.timezone("Asia/Singapore")
            full_datetime_str = f"{datetime.now().year}/{date_part} {time_part}"
            dt = sg_timezone.localize(datetime.strptime(full_datetime_str, "%Y/%d/%m %H:%M"))
            return pd.Series([dt, desc_part])
        else:
            return pd.Series([pd.NaT, msg])
    
    # Apply split function
    df[['incident_time', 'incident_description']] = df['Message'].apply(split_message)
    
    # Rename and select columns
    df_cleaned = df[['Type', 'Latitude', 'Longitude', 'incident_time', 'incident_description']].rename(
        columns={
            'Type': 'type',
            'Latitude': 'latitude',
            'Longitude': 'longitude'
        }
    )
    
    return df_cleaned

def transform_sg_public_holidays(dfs):
    """Transform public holiday data into structured dataframe"""
    transformed = []
    for df in dfs:
        if '_id' in df.columns:
            df = df.drop(columns=['_id'])
        transformed.append(df)
    
    # Combine all cleaned DataFrames
    df_combined = pd.concat(transformed, ignore_index=True)
    
    # Check if 'date' column exists (case-insensitive)
    date_col = next((col for col in df_combined.columns if col.lower() == "date"), None)
    
    if date_col:
        # Convert to datetime
        df_combined[date_col] = pd.to_datetime(df_combined[date_col], errors='coerce')
    
        # Sort by date in descending order
        df_combined = df_combined.sort_values(by=date_col, ascending=False).reset_index(drop=True)
    
    return df_combined

# ===== MAIN EXECUTION FUNCTION =====

def main():
    """Main execution function to demonstrate ETL workflow"""
    # API keys
    ura_access_key = "fb1d226d-6f7f-4ec4-b678-247dee275ca7"
    stb_api_key = "r4VgRmHwVhs5uG1WlqXzprvFWCSK8lyf"
    lta_account_key = "brlj8fVmS+u/X9vpSjismQ=="
    sg_holiday_dataset_ids = [
        "d_3751791452397f1b1c80c451447e40b7",
        "d_4e19214c3a5288eab7a27235f43da4fa",
        "d_0b0fe4a7101674be0a359e55d32cd126"
    ]
    
    # Extract data
    print("Extracting data...")
    ura_data = extract_ura_data(ura_access_key)
    weather_data = extract_weather_data()
    events_data = extract_events_data(stb_api_key)
    traffic_data = extract_traffic_incidents(lta_account_key)
    holiday_data = extract_sg_public_holidays(sg_holiday_dataset_ids)
    
    # Transform data
    print("Transforming data...")
    ura_dfs = transform_ura_data(ura_data)
    weather_df = transform_weather_data(weather_data)
    events_df = transform_events_data(events_data)
    traffic_df = transform_traffic_incidents(traffic_data)
    holiday_df = transform_sg_public_holidays(holiday_data)
    
    # Display results (for demonstration)
    print("\nURA Car Park Availability (First 5):")
    print(ura_dfs["availability"].head())
    
    print("\nURA Car Park List and Rates (First 5):")
    print(ura_dfs["car_park_list"].head())
    
    print("\nURA Season Car Park List and Rates (First 5):")
    print(ura_dfs["season_car_park_list"].head())
    
    print("\nRainfall Data:")
    print(weather_df.head())
    
    print("\nEvents Data:")
    print(events_df.head())
    
    print("\nTraffic Incidents:")
    print(traffic_df.head())
    
    print("\nPublic Holidays:")
    print(holiday_df.head())

if __name__ == "__main__":
    main()