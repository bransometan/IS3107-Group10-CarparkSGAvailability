import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import folium
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime
import os

# Configuration
KEY_PATH = '/keys/is3107-457309-0e9066063708.json'
DATASET_ID = 'singapore_datasets'
SAVE_CHARTS = True
OUTPUT_DIR = 'reports'

# Create output directory if it doesn't exist
if SAVE_CHARTS and not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

def setup_bigquery_client():
    """Setup and return BigQuery client"""
    credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    return client, credentials.project_id

def run_query(client, query):
    """Run a BigQuery query and return results as a dataframe"""
    try:
        return client.query(query).to_dataframe()
    except Exception as e:
        print(f"Error running query: {str(e)}")
        return pd.DataFrame()

def get_top_carparks_by_availability(client, project_id, limit=10):
    """Query BigQuery for top car parks with most available lots"""
    query = f"""
    SELECT 
        a.carparkNo, 
        MAX(a.lotsAvailable) as available_lots,
        MAX(c.carparkName) as carpark_name,
        MAX(a.latitude) as latitude,
        MAX(a.longitude) as longitude
    FROM 
        `{project_id}.{DATASET_ID}.ura_carpark_availability` a
    LEFT JOIN 
        `{project_id}.{DATASET_ID}.ura_carpark_list` c
    ON 
        a.carparkNo = c.carparkNo
    GROUP BY 
        a.carparkNo
    ORDER BY 
        available_lots DESC
    LIMIT {limit}
    """
    
    return run_query(client, query)

def get_carpark_availability_region(client, project_id):
    query = f"""
    SELECT 
        carparkNo, 
        lotsAvailable, 
        CAST(datetime as DATETIME) as datetime,
        latitude,
        longitude
    FROM 
        `{project_id}.{DATASET_ID}.ura_carpark_availability`
    ORDER BY 
        datetime ASC
    """
    return run_query(client, query) 

def get_carpark_utilization(client, project_id, limit=10):
    """Query BigQuery for car park utilization rates"""
    query = f"""
    WITH Availability AS (
      SELECT 
        a.carparkNo, 
        MAX(a.lotsAvailable) as available_lots,
        MAX(c.carparkName) as carpark_name,
        MAX(c.parkCapacity) as capacity,
        MAX(a.latitude) as latitude,
        MAX(a.longitude) as longitude
      FROM 
        `{project_id}.{DATASET_ID}.ura_carpark_availability` a
      LEFT JOIN 
        `{project_id}.{DATASET_ID}.ura_carpark_list` c
      ON 
        a.carparkNo = c.carparkNo
      WHERE
        c.parkCapacity > 0
      GROUP BY 
        a.carparkNo
    )
    
    SELECT
      carparkNo,
      carpark_name,
      available_lots,
      capacity,
      latitude,
      longitude,
      ROUND((capacity - available_lots) / capacity * 100, 2) as utilization_rate
    FROM
      Availability
    WHERE
      capacity > 0
    ORDER BY
      utilization_rate DESC
    LIMIT {limit}
    """
    
    return run_query(client, query)

def get_bubble_chart_data(client, project_id):
    query = f"""
    SELECT 
        carparkNo, 
        weekdayRate, 
        parkCapacity, 
        vehCat
    FROM 
        `{project_id}.{DATASET_ID}.ura_carpark_list`
    """
    return run_query(client, query)

def get_rainfall_data(client, project_id, limit=30):
    """Query BigQuery for rainfall data"""
    query = f"""
    SELECT 
        name, 
        latitude, 
        longitude, 
        rainfall_amount,
        CAST(datetime as DATETIME) as datetime
    FROM 
        `{project_id}.{DATASET_ID}.weather_rainfall`
    ORDER BY 
        datetime DESC
    LIMIT {limit}
    """
    return run_query(client, query)

def get_traffic_incidents(client, project_id, limit=50):
    """Query BigQuery for traffic incidents"""
    query = f"""
    SELECT 
        type, 
        latitude, 
        longitude, 
        incident_description,
        incident_time
    FROM 
        `{project_id}.{DATASET_ID}.traffic_incidents`
    ORDER BY 
        incident_time DESC
    LIMIT {limit}
    """
    
    return run_query(client, query)

def get_events_data(client, project_id, limit=20):
    """Query BigQuery for events data"""
    query = f"""
    SELECT 
        name, 
        type,
        description,
        latitude, 
        longitude, 
        ticketed,
        rating,
        nearestMRT,
        datetime
    FROM 
        `{project_id}.{DATASET_ID}.events`
    WHERE
        latitude IS NOT NULL AND longitude IS NOT NULL
    LIMIT {limit}
    """
    
    return run_query(client, query)

def plot_top_carparks(df, title="Top Car Parks by Available Lots", save_path=None):
    """Create a bar chart of top car parks by availability"""
    if save_path is None:
        save_path="reports/top_carparks_avail.png"
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    plt.figure(figsize=(12, 8))
    
    # Set style
    sns.set_style("whitegrid")
    
    # Create bar plot
    ax = sns.barplot(
        x='carpark_name', 
        y='available_lots', 
        data=df,
        palette='viridis'
    )
    
    # Add labels and title
    plt.title(title, fontsize=16, pad=20)
    plt.xlabel('Car Park', fontsize=14)
    plt.ylabel('Available Lots', fontsize=14)
    plt.xticks(rotation=45, ha='right', fontsize=12)
    plt.yticks(fontsize=12)
    
    # Add count labels on bars
    for i, row in enumerate(df.itertuples()):
        ax.text(
            i, 
            row.available_lots + 5, 
            str(row.available_lots),
            ha='center', 
            fontsize=12, 
            fontweight='bold'
        )
    
    # Adjust layout
    plt.tight_layout()
    
    # Save or show figure
    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        print(f"Chart saved to {save_path}")
    
    return plt
    #return {"save_path":save_path} 

def plot_carpark_utilization(df, title="Top Car Parks by Utilization Rate", save_path=None):
    """Create a bar chart of car parks by utilization rate"""
    if save_path is None:
        save_path="reports/top_carparks_util.png"
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    plt.figure(figsize=(12, 8))
    
    # Set style
    sns.set_style("whitegrid")
    
    # Create bar plot
    ax = sns.barplot(
        x='carpark_name', 
        y='utilization_rate', 
        data=df,
        palette='plasma'
    )
    
    # Add labels and title
    plt.title(title, fontsize=16, pad=20)
    plt.xlabel('Car Park', fontsize=14)
    plt.ylabel('Utilization Rate (%)', fontsize=14)
    plt.xticks(rotation=45, ha='right', fontsize=12)
    plt.yticks(fontsize=12)
    
    # Add percentage labels on bars
    for i, row in enumerate(df.itertuples()):
        ax.text(
            i, 
            min(row.utilization_rate + 2, 100), 
            f"{row.utilization_rate:.1f}%",
            ha='center', 
            fontsize=11, 
            fontweight='bold'
        )
    
    # Adjust layout
    plt.tight_layout()
    
    # Save or show figure
    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        print(f"Chart saved to {save_path}")
    
    return plt

# Bubble Chart of Car Park Rates vs. Parking Capacity
def plot_carpark_rates_vs_capacity(df_carparklist, save_path=None):
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    df_carparklist['weekdayRate'] = df_carparklist['weekdayRate'].replace({'\$': '', ',': ''}, regex=True)

    df_carparklist['weekdayRate'] = pd.to_numeric(df_carparklist['weekdayRate'], errors='coerce')
    df_carparklist['weekdayRate'] = df_carparklist['weekdayRate'].astype(float) 

    df_carparklist['parkCapacity'] = pd.to_numeric(df_carparklist['parkCapacity'], errors='coerce')
    df_carparklist['parkCapacity'] = df_carparklist['parkCapacity'].astype(float)



    plt.figure(figsize=(12, 6))
    sns.scatterplot(data=df_carparklist, x='weekdayRate', y='parkCapacity', size='parkCapacity', sizes=(20, 200), hue='vehCat', palette='Set2')
    plt.title('Car Park Rates vs. Parking Capacity')
    plt.xlabel('Weekday Parking Rate ($)')
    plt.ylabel('Parking Capacity')
    plt.tight_layout()
    #plt.show()
    return plt

def plot_carparks_by_region_pie(df, save_path=None):
    """
    Create a pie chart showing the distribution of car parks by region.
    """
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    # Count car parks by region
    region_counts = df['region'].value_counts()
    
    # Plot the pie chart
    plt.figure(figsize=(8, 8))
    plt.pie(
        region_counts,
        labels=region_counts.index,
        autopct='%1.1f%%',  # Show percentages
        startangle=90,      # Rotate the chart to start at 90 degrees
        colors=['lightblue', 'lightgreen', 'lightcoral', 'gold'],  # Colors for regions
        wedgeprops={'edgecolor': 'white'}  # Add white edges for better visibility
    )
    
    # Add title
    plt.title("Distribution of Car Parks by Region", fontsize=16, pad=20)
    
    # Adjust layout
    plt.tight_layout()
    
    # Save or show the figure
    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        print(f"Pie chart saved to {save_path}")
    #else:
    #    plt.show()
    return plt

def plot_rainfall_distribution(df, title="Rainfall Distribution", save_path=None):
    """Create a bar chart of rainfall distribution"""
    if save_path is None:
        save_path="reports/rainfall_dist.png"
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    plt.figure(figsize=(14, 8))
    
    # Set style
    sns.set_style("whitegrid")
    
    # Sort by rainfall amount
    df_sorted = df.sort_values('rainfall_amount', ascending=False)
    
    # Create bar plot
    ax = sns.barplot(
        x='name',
        y='rainfall_amount',
        data=df_sorted,
        palette='Blues_r'
    )
    
    # Add labels and title
    plt.title(title, fontsize=16, pad=20)
    plt.xlabel('Weather Station', fontsize=14)
    plt.ylabel('Rainfall Amount (mm)', fontsize=14)
    plt.xticks(rotation=45, ha='right', fontsize=10)
    plt.yticks(fontsize=12)
    
    # Add value labels on bars
    for i, row in enumerate(df_sorted.itertuples()):
        if row.rainfall_amount > 0:
            ax.text(
                i,
                row.rainfall_amount + 0.1,
                f"{row.rainfall_amount:.2f}",
                ha='center',
                fontsize=9,
                fontweight='bold'
            )
    
    # Adjust layout
    plt.tight_layout()
    
    # Save or show figure
    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        print(f"Chart saved to {save_path}")
    
    return plt

def plot_traffic_incidents_by_type(df, title="Traffic Incidents by Type", save_path=None):
    """Create a pie chart of traffic incidents by type"""
    if save_path is None:
        save_path="reports/traffic_inci.png"
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    plt.figure(figsize=(10, 10))
    
    # Count incidents by type
    incident_counts = df['type'].value_counts()
    
    # Create pie chart
    plt.pie(
        incident_counts,
        labels=incident_counts.index,
        autopct='%1.1f%%',
        startangle=90,
        colors=sns.color_palette('Set3', len(incident_counts)),
        wedgeprops={'edgecolor': 'w', 'linewidth': 1}
    )
    
    # Add title
    plt.title(title, fontsize=16, pad=20)
    
    # Equal aspect ratio ensures that pie is drawn as a circle
    plt.axis('equal')
    
    # Save or show figure
    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        print(f"Chart saved to {save_path}")
    
    #return plt
    return plt

def plot_events_by_type(df, title="Events by Type", save_path=None):
    """Create a bar chart of events by type"""
    if save_path is None:
        save_path="reports/events_type.png"
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    plt.figure(figsize=(12, 8))
    
    # Count events by type
    if 'type' in df.columns:
        event_counts = df['type'].value_counts()
        
        # Set style
        sns.set_style("whitegrid")
        
        # Create bar plot
        ax = sns.barplot(
            x=event_counts.index,
            y=event_counts.values,
            palette='Set2'
        )
        
        # Add labels and title
        plt.title(title, fontsize=16, pad=20)
        plt.xlabel('Event Type', fontsize=14)
        plt.ylabel('Count', fontsize=14)
        plt.xticks(rotation=45, ha='right', fontsize=12)
        plt.yticks(fontsize=12)
        
        # Add count labels on bars
        for i, v in enumerate(event_counts.values):
            ax.text(
                i,
                v + 0.2,
                str(v),
                ha='center',
                fontsize=12,
                fontweight='bold'
            )
        
        # Adjust layout
        plt.tight_layout()
        
        # Save or show figure
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            print(f"Chart saved to {save_path}")
        
        return plt
    else:
        print("No 'type' column found in events data")
        return None

def create_carpark_map(df, output_path=None):
    """Create an HTML map with car parks"""
    # Create a map centered on Singapore
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    m = folium.Map(location=[1.3521, 103.8198], zoom_start=12, tiles="cartodbpositron")
    
    # Add car parks as circle markers
    for _, row in df.iterrows():
        if pd.notna(row['latitude']) and pd.notna(row['longitude']):
            # Create popup text
            popup_text = f"""
            <b>{row['carpark_name']}</b><br>
            Available Lots: {row['available_lots']}
            """
            
            # Create circle marker
            folium.CircleMarker(
                location=[row['latitude'], row['longitude']],
                radius=8,
                popup=folium.Popup(popup_text, max_width=300),
                tooltip=row['carpark_name'],
                color='green',
                fill=True,
                fill_color='green',
                fill_opacity=0.7
            ).add_to(m)
    
    # Save map to HTML file
    if output_path:
        m.save(output_path)
        print(f"Map saved to {output_path}")
    
    return m

def plot_regional_availability_trends(df, save_path=None):
    """Plot aggregated availability trends by region."""
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    df = df.groupby(['datetime', 'region'])['lotsAvailable'].mean().reset_index()
    plt.figure(figsize=(12, 6))
    sns.lineplot(data=df, x='datetime', y='lotsAvailable', hue='region', palette='Set2')
    plt.title("Regional Car Park Availability Trends Over Time", fontsize=16)
    plt.xlabel("Time", fontsize=14)
    plt.ylabel("Average Available Lots", fontsize=14)
    plt.xticks(rotation=45)
    plt.tight_layout()
    #plt.show()
    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        print(f"Chart saved to {save_path}")
    
    #return plt
    return plt

def plot_availability_by_region(df,save_path=None):
    """Plot availability trends for each region in separate subplots."""
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    regions = df['region'].unique()
    fig, axes = plt.subplots(len(regions), 1, figsize=(12, 6 * len(regions)), sharex=True)
    for i, region in enumerate(regions):
        region_df = df[df['region'] == region]
        region_df = region_df.groupby('datetime')['lotsAvailable'].mean().reset_index()
        sns.lineplot(data=region_df, x='datetime', y='lotsAvailable', ax=axes[i], color='blue')
        axes[i].set_title(f"Car Park Availability Trends in {region}", fontsize=14)
        axes[i].set_ylabel("Average Available Lots", fontsize=12)
        axes[i].tick_params(axis='x', rotation=45)
    plt.xlabel("Time", fontsize=14)
    plt.tight_layout()
    #plt.show()
    return plt

def assign_region(lat, lon):
    """
    Assign a region (North, South, East, West, Central) based on latitude and longitude.
    """
    # Define Central region boundaries
    if 1.28 <= lat <= 1.32 and 103.83 <= lon <= 103.86:  # Central
        return "Central"
    elif lat > 1.356 and lon > 103.825:  # North
        return "North"
    elif lat < 1.27 and lon > 103.825:  # South
        return "South"
    elif lon > 103.9:  # East
        return "East"
    else:  # West
        return "West"

def main():
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    print(f"=== Starting visualization process at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===")
    
    # Setup BigQuery client
    client, project_id = setup_bigquery_client()
    
    # Process car park data
    print("\n=== Processing Car Park Data ===")
    top_availability_df = get_top_carparks_by_availability(client, project_id)
    utilization_df = get_carpark_utilization(client, project_id)
    
    if not top_availability_df.empty:
        print("Creating car park availability chart...")
        plot_top_carparks(
            top_availability_df,
            save_path=f"{OUTPUT_DIR}/carpark_availability_{timestamp}.png" if SAVE_CHARTS else None
        )
        
        print("Creating car park map...")
        create_carpark_map(
            top_availability_df,
            output_path=f"{OUTPUT_DIR}/carpark_map_{timestamp}.html" if SAVE_CHARTS else None
        )
    else:
        print("No car park availability data found")
    
    if not utilization_df.empty:
        print("Creating car park utilization chart...")
        plot_carpark_utilization(
            utilization_df,
            save_path=f"{OUTPUT_DIR}/carpark_utilization_{timestamp}.png" if SAVE_CHARTS else None
        )
    else:
        print("No car park utilization data found")    
    

     # Process car park data
    print("\n=== Processing Car Park Data ===")
    availability_df = get_carpark_availability_region(client, project_id)
    print(availability_df.head())

    # Add region column
    availability_df['region'] = availability_df.apply(
        lambda row: assign_region(row['latitude'], row['longitude']), axis=1
        )
    
    print(availability_df.head())
    print('Test 1: ', availability_df['datetime'].head())
    print('Test 2: ', availability_df.groupby('datetime').size())

    
    if not availability_df.empty:
        print("Plotting regional car park availability trends...")
        plot_regional_availability_trends(availability_df, save_path=f"{OUTPUT_DIR}/regional_availability_trends_{timestamp}.png" if SAVE_CHARTS else None)
        print("Plotting pie chart of region's car parks...")
        plot_carparks_by_region_pie(availability_df, save_path=f"{OUTPUT_DIR}/carpark_availability_by_region_pie_{timestamp}.png" if SAVE_CHARTS else None)
        
    else:
        print("No car park availability data found")
    
    bubble_df = get_bubble_chart_data(client, project_id)

    if not bubble_df.empty:
        print("Creating car park rates vs. capacity bubble chart...")
        plot_carpark_rates_vs_capacity(
            bubble_df,
            save_path=f"{OUTPUT_DIR}/carpark_rates_vs_capacity_{timestamp}.png" if SAVE_CHARTS else None)
    else:
        print("No car park list data found")


        
    # Process rainfall data
    print("\n=== Processing Rainfall Data ===")
    rainfall_df = get_rainfall_data(client, project_id)
    
    if not rainfall_df.empty:
        print("Creating rainfall distribution chart...")
        plot_rainfall_distribution(
            rainfall_df,
            save_path=f"{OUTPUT_DIR}/rainfall_distribution_{timestamp}.png" if SAVE_CHARTS else None
        )
    else:
        print("No rainfall data found")
    
    # Process traffic incidents
    print("\n=== Processing Traffic Incidents ===")
    traffic_df = get_traffic_incidents(client, project_id)
    
    if not traffic_df.empty:
        print("Creating traffic incidents chart...")
        plot_traffic_incidents_by_type(
            traffic_df,
            save_path=f"{OUTPUT_DIR}/traffic_incidents_{timestamp}.png" if SAVE_CHARTS else None
        )
    else:
        print("No traffic incidents data found")
    
    # Process events data
    print("\n=== Processing Events Data ===")
    events_df = get_events_data(client, project_id)
    
    if not events_df.empty:
        print("Creating events chart...")
        plot_events_by_type(
            events_df,
            save_path=f"{OUTPUT_DIR}/events_by_type_{timestamp}.png" if SAVE_CHARTS else None
        )
    else:
        print("No events data found")

    client.close()
    print(f"\n=== Visualization process completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===")
    
    # Display all figures (if not in a non-interactive environment)
    return {"parent_dir":"/opt/airflow/reports"}

if __name__ == "__main__":
    main()