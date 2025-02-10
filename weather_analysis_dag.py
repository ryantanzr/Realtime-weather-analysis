import requests, json, time

from airflow.decorators import dag, task 
from airflow.sensors.base import PokeReturnValue
from airflow.models import Variable
from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 27),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def produce_periodic_weather_data(periodic_data: list):
    for data in periodic_data:
        yield json.dumps(data)

def consume_periodic_weather_data(message, name):
    key = json.loads(message.key())
    message_content = json.loads(message.value())
    print(f"Consumed periodic event with key={key} and value={message_content}")

# This dag incorporates producer and consumer tasks that cover the Extraction and Ingestion steps
# of the ELT process. The DAG is simple, we produce messages to kafka topics continuously, only
# consuming them when a custom sensor has tracked > 10 messages in the topics, which will
# trigger the Loading step of the ELT process to run.
@dag(dag_id='producer_consumer_dag',
    schedule_interval='*/5 * * * *',
    default_args=default_args,
    catchup=False,
    tags=['weather_data'])
def weather_analysis_dag():

    # The extract_weather_data task extracts the weather data from the API
    @task(task_id="extract_weather_data")
    def extract_weather_data():

        headers = {
        "Referer": 'https://www.amazon.com/',
        "Sec-Ch-Ua": "Not_A Brand",
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": "macOS",
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1.1 Safari/605.1.15'
        }
        
        url = "https://api-open.data.gov.sg/v2/real-time/api/twenty-four-hr-forecast"
        response = requests.get(url)

        # if the response is successful, start extracting the data
        if response.status_code == 200:
            
            # access the content of the response
            content = response.json()    

            # extract the data from the content
            data_records = content['data']['records'][0]

            # further extraction into general and periodic data
            periodic_data = data_records["periods"]

            # Extract the periodic data
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

                extraction_count = int(Variable.get("extraction_count", default_var=0))
                extraction_count = extraction_count + 1
                Variable.set("extraction_count", extraction_count)
            
            return periodic_datas
        
    data = extract_weather_data()

weather_analysis_dag()
