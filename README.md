# IS3107-Group10-CarparkSGAvailability

This project provides tools for scraping, transforming, and visualizing car park availability, weather, traffic, and events data in Singapore. It also includes functionality to upload the processed data to Google BigQuery.

## Prerequisites

1. Install Python
2. Ensure you have `pip` installed.
3. Set up a Google Cloud project with BigQuery enabled.
4. Download your Google Cloud service account key file and place it in the `key/` directory as `is3107-457309-0e9066063708.json`.

## Setup Instructions

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/bransometan/IS3107-Group10-CarparkSGAvailability.git
   cd IS3107-Group10-CarparkSGAvailability
   ```

2. **Install Dependencies**:
   Install the required Python packages using `requirements.txt`:
   ```bash
   pip install -r requirements.txt
   ```

3. **Set Up Environment**:
   Ensure the service account key file is located at `key/is3107-457309-0e9066063708.json`.

## Running the Scripts

### 1. **Scraping Data**
   The `scrape_all_data.py` script extracts and transforms data from various APIs:
   ```bash
   python scrape_all_data.py
   ```
   This script fetches data for:
   - URA car park availability
   - Weather (rainfall)
   - Events
   - Traffic incidents
   - Public holidays

   It outputs transformed data as pandas DataFrames.

### 2. **Uploading Data to BigQuery**
   The `upload_all.py` script uploads all the transformed data to BigQuery:
   ```bash
   python upload_all.py
   ```
   This script:
   - Sets up the BigQuery client.
   - Creates the dataset `singapore_datasets` if it doesn't exist.
   - Uploads the data to the respective tables in BigQuery.

   You can also run individual upload scripts for specific datasets:
   - **URA Data**: `python upload_ura.py`
   - **Weather Data**: `python upload_weather.py`
   - **Traffic Data**: `python upload_traffic.py`
   - **Events Data**: `python upload_events.py`
   - **Public Holidays**: `python upload_holidays.py`

### 3. **Visualizing Data**
   The `dashboard_app.py` script generates visualizations and maps:
   ```bash
   python dashboard_app.py
   ```
   This script:
   - Queries data from BigQuery.
   - Generates charts for car park availability, utilization, rainfall distribution, traffic incidents, and events.
   - Creates an interactive map of car parks.
   - Saves visualizations to the `reports/` directory.

## Directory Structure

```
IS3107-Group10-CarparkSGAvailability/
├── bigquery_utils.py        # Utility functions for BigQuery operations
├── dashboard_app.py         # Visualization and dashboard generation
├── scrape_all_data.py       # Data extraction and transformation
├── upload_all.py            # Upload all datasets to BigQuery
├── upload_ura.py            # Upload URA data to BigQuery
├── upload_weather.py        # Upload weather data to BigQuery
├── upload_traffic.py        # Upload traffic data to BigQuery
├── upload_events.py         # Upload events data to BigQuery
├── upload_holidays.py       # Upload public holiday data to BigQuery
├── requirements.txt         # Python dependencies
├── key/                     # Directory for service account key (Bigquery)
│   └── is3107-457309-0e9066063708.json
└── reports/                 # Directory for generated charts and maps
```

## Notes

- Ensure your API keys are valid and have sufficient quota for the respective APIs.
- The `reports/` directory will be created automatically if it doesn't exist when running dashboard_app.py.
- For any issues, refer to the error messages printed in the console.
