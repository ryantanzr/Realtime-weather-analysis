import requests, json, time, logging, sys, six

from airflow.decorators import dag, task 
from airflow.sensors.base import PokeReturnValue
from airflow.models import Variable
from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime, timedelta
from cassandra.cluster import Cluster

if sys.version_info >= (3, 12, 0):
    sys.modules['kafka.vendor.six.moves'] = six.moves

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 27),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

DURATION = 60
MAX_MESSAGES = 5
KAKFA_TOPIC = 'periodic_weather'
KEYSPACE = 'weather'

# This dag incorporates producer and consumer tasks that cover the Extraction and Ingestion steps
# of the ELT process. The DAG is simple, we produce messages to kafka topics continuously, only
# consuming them when a custom sensor has tracked > 10 messages in the topics, which will
# trigger the Loading step of the ELT process to run.
@dag(dag_id='producer_consumer_dag',
    schedule_interval='*/1 * * * *',
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
    
    # Function to continuously send messages to Kafka
    @task(task_id="produce_periodic_weather_data")
    def produce_periodic_weather_data(periodic_data: list):

        logging.info("Sending periodic weather data to Kafka...")

        producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)

        for data in periodic_data:
            try:
                producer.send(KAKFA_TOPIC, json.dumps(data).encode('utf-8'))
                logging.info(f"Sent data to Kafka: {data}")
                time.sleep(1)  # Sleep for 1 second to prevent flooding
            except Exception as e:
                logging.error(f"An error occurred while sending data to Kafka: {e}")

        producer.close()
        logging.info("Finished sending user data to Kafka.")

    # Sensor to track the number of messages in the Kafka topic
    @task.sensor(poke_interval=30, timeout=5, mode="reschedule")
    def data_sensor() -> PokeReturnValue:
        produced_messages = int(Variable.get("extraction_count", default_var=0))
        res = (produced_messages % 2 == 0)
        return PokeReturnValue(is_done=res)
    
    # Function to consume messages from Kafka
    @task(task_id="consume_periodic_weather_data")
    def consume_periodic_weather_data():
        
        # Setup Kafka consumer
        consumer = KafkaConsumer(
            KAKFA_TOPIC,
            bootstrap_servers=['broker:29092'],
            auto_offset_reset='earliest',
            enable_auto_commit=False,  # Manually commit offsets
            group_id='my-group'
        )

        # Set up a session to connect to cassandra
        cluster = Cluster(['cassandra'])
        session = cluster.connect()

        # Create keyspace and table if they don't exist
        session.execute("""
        CREATE KEYSPACE IF NOT EXISTS weather WITH REPLICATION = {
            'class'              : 'SimpleStrategy',
            'replication_factor' : 1
        }""")
        session.set_keyspace(KEYSPACE)

        session.execute("""
        CREATE TABLE IF NOT EXISTS periodic (
            id UUID PRIMARY KEY,
            start_time_period TEXT,
            end_time_period TEXT,
            north_forecast TEXT,
            south_forecast TEXT,
            east_forecast TEXT,
            west_forecast TEXT,
            central_forecast TEXT
        )""")
        
        # Poll for messages for a specified time
        end_time = time.time() + DURATION

        while time.time() < end_time:

            # Poll with a 5-second timeout
            messages = consumer.poll(timeout_ms=5000, max_records= MAX_MESSAGES) 

            if messages:
                for topic_partition, messages in messages.items():
                    for message in messages:

                        # Decode the Kafka message
                        data = json.loads(message.value.decode('utf-8'))
                        print(f"Consumed message and topic: {message.topic} with value: {data}")

                        # Insert the data into Cassandra
                        session.execute("""
                        INSERT INTO weather.periodic (id, start_time_period, end_time_period, north_forecast, south_forecast, east_forecast, west_forecast, central_forecast)
                        VALUES (uuid(), %s, %s, %s, %s, %s, %s, %s)
                        """, (data['start_time_period'], data['end_time_period'], data['north_forecast'], data['south_forecast'], data['east_forecast'], data['west_forecast'], data['central_forecast'],))

                # Manually commit the Kafka offset to avoid reprocessing
                consumer.commit()

            else:
                logging.info("No messages consumed within the polling period.")

        logging.info("Time limit reached. Stopping consumer...")

    
    data = extract_weather_data()
    produce_periodic_weather_data(data)
    data_sensor() >> consume_periodic_weather_data()


weather_analysis_dag()
