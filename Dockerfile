# Extend the official Airflow image
FROM apache/airflow:2.10.4

# Copy and install the requirements in the text file
COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt