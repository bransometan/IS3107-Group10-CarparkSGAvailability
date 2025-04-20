# Use the official Airflow image as a base
FROM apache/airflow:2.10.5

# Install additional Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

