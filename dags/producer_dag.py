import requests, json, time, logging, sys, six

from airflow.decorators import dag, task, task_group
from airflow.sensors.base import PokeReturnValue
from airflow.models import Variable
from kafka import KafkaProducer
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 27),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

DURATION = 60
KAKFA_TOPICS = ['periodic_weather', 'general_weather']
KEYSPACE = 'weather'

# This dag incorporates producer and consumer tasks that cover the Extraction and Ingestion steps
# of the ELT process. The DAG is simple, we produce messages to kafka topics continuously, only
# consuming them when a custom sensor has tracked > 10 messages in the topics, which will
# trigger the Loading step of the ELT process to run.
@dag(dag_id='weather_producer_dag',
     schedule_interval='*/5 * * * *',
     default_args=default_args,
     catchup=False,
     tags=['Ingestion'])
def producer_dag():

    # Extracts the payload from the API call
    @task(task_id="extract_payload")
    def extract_payload():

        url = "https://api-open.data.gov.sg/v2/real-time/api/twenty-four-hr-forecast"
        response = requests.get(url)

        # if the response is successful, start extracting the data
        if response.status_code != 200:
            logging.error(f"Failed to fetch data from the API. Status code: {response.status_code}")
    
        return response.json()


    # Extracts the general weather data from the payload
    @task(task_id="extract_general_data")
    def extract_general_data(payload: dict):

        data_records = payload['data']['records'][0]

        updated_Timestamp = data_records["updatedTimestamp"]

        temperatures = data_records["general"]["temperature"]
        low_temp, high_temp = temperatures["low"], temperatures["high"]

        humidity = data_records["general"]["relativeHumidity"]
        low_humidity, high_humidity = humidity["low"], humidity["high"]

        general_forecast = data_records["general"]["forecast"]["text"]

        wind = data_records["general"]["wind"]
        low_wind_speed, high_wind_speed = wind["speed"]["low"], wind["speed"]["high"]

        wind_direction = wind["direction"]

        return {
            "updated_Timestamp": updated_Timestamp,
            "low_temp": low_temp,
            "high_temp": high_temp,
            "low_humidity": low_humidity,
            "high_humidity": high_humidity,
            "general_forecast": general_forecast,
            "low_wind_speed": low_wind_speed,
            "high_wind_speed": high_wind_speed,
            "wind_direction": wind_direction
        }

    # Extracts the periodic weather data from the payload
    @task(task_id="extract_periodic_data")
    def extract_periodic_data(payload: dict):

        data_records = payload['data']['records'][0]
        periodic_data = data_records["periods"]

        periodic_datas = []
        for i in range(len(periodic_data)):
            time_period = periodic_data[i]["timePeriod"]
            start_time_period, end_time_period = time_period["start"], time_period["end"]

            regions = periodic_data[i]["regions"]
            north_forecast, south_forecast, east_forecast, west_forecast, central_forecast = regions["north"]["text"], regions["south"]["text"], regions["east"]["text"], regions["west"]["text"], regions["central"]["text"]
            
            data = {
                "start_time_period": start_time_period,
                "end_time_period": end_time_period,
                "north_forecast": north_forecast,
                "south_forecast": south_forecast,
                "east_forecast": east_forecast,
                "west_forecast": west_forecast,
                "central_forecast": central_forecast
            }

            periodic_datas.append(data)
        
        return periodic_datas
    
    # Function to continuously send messages to Kafka
    @task(task_id="produce_periodic_weather_data")
    def produce_periodic_weather_data(periodic_data: list):

        logging.info("Sending periodic weather data to Kafka...")

        producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)

        for data in periodic_data:
            try:
                producer.send(KAKFA_TOPICS[0], json.dumps(data).encode('utf-8'))
                logging.info(f"Sent data to Kafka: {data}")
                time.sleep(1)  # Sleep for 1 second to prevent flooding
            except Exception as e:
                logging.error(f"An error occurred while sending data to Kafka: {e}")

        producer.close()
        logging.info("Finished sending user data to Kafka.")
    
    # Function to continuously send messages to Kafka
    @task(task_id="produce_general_weather_data")
    def produce_general_weather_data(general_data: dict):

        logging.info("Sending general weather data to Kafka...")

        producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)

        try:
            producer.send(KAKFA_TOPICS[1], json.dumps(general_data).encode('utf-8'))
            logging.info(f"Sent data to Kafka: {general_data}")
        except Exception as e:
            logging.error(f"An error occurred while sending data to Kafka: {e}")
    
    @task(group_id="update_airflow_variables")
    def update_airflow_variables():
        extraction_count = int(Variable.get("extraction_count", default_var=0))
        Variable.set("extraction_count", extraction_count + 1)



    @task_group(group_id="produce_weather_data")
    def produce_weather_data(general_data: dict, periodic_data: list):
        produce_periodic_weather_data(periodic_data)
        produce_general_weather_data(general_data)
    

    payload = extract_payload()
    general_data = extract_general_data(payload)
    periodic_data = extract_periodic_data(payload)

    produce_weather_data(general_data, periodic_data)

producer_dag()
