import requests, json, time, logging, sys, six

from airflow.decorators import dag, task, task_group
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
KAKFA_TOPICS = ['periodic_weather', 'general_weather']
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

    # Sensor to track the number of messages in the Kafka topic
    @task.sensor(poke_interval=30, timeout=5, mode="reschedule")
    def data_sensor() -> PokeReturnValue:
        produced_messages = int(Variable.get("extraction_count", default_var=0))
        res = (produced_messages % 2 == 0)
        return PokeReturnValue(is_done=res)
    
    # Sensor to create the keyspace and table in Cassandra
    @task(task_id="create_cassandra_keyspace_table")
    def create_cassandra_keyspace_table():
        cluster = Cluster(['cassandra'])
        session = cluster.connect()

        session.execute("""
        CREATE KEYSPACE IF NOT EXISTS weather WITH REPLICATION = {
            'class'              : 'SimpleStrategy',
            'replication_factor' : 1
        }""")

        logging.info("Created keyspace 'weather'")

        session.set_keyspace(KEYSPACE)

        session.execute("""
        CREATE TABLE IF NOT EXISTS general (
            id UUID PRIMARY KEY,
            updated_Timestamp TEXT,
            low_temp TEXT,
            high_temp TEXT,
            low_humidity TEXT,
            high_humidity TEXT,
            general_forecast TEXT,
            low_wind_speed TEXT,
            high_wind_speed TEXT,
            wind_direction TEXT
        )""")

        logging.info("Created table 'general'")

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

        logging.info("Created table 'periodic'")

        session.shutdown()
        

    # Function to consume messages from Kafka
    @task(task_id="consume_periodic_weather_data")
    def consume_periodic_weather_data():
        
        # Setup Kafka consumer
        consumer = KafkaConsumer(
            KAKFA_TOPICS[0],
            bootstrap_servers=['broker:29092'],
            auto_offset_reset='earliest',
            enable_auto_commit=False,  # Manually commit offsets
            group_id='my-group'
        )

        # Set up a session to connect to cassandra
        cluster = Cluster(['cassandra'])
        session = cluster.connect().set_keyspace(KEYSPACE)

      
        # Poll for messages for a specified time
        end_time = time.time() + DURATION

        while time.time() < end_time:

            # Poll with a 5-second timeout
            messages = consumer.poll(timeout_ms=5000, max_records=5) 

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
        
        consumer.close()
        session.shutdown()

    # Function to consume messages from Kafka
    @task(task_id="consume_general_weather_data")
    def consume_general_weather_data():
        
        # Setup Kafka consumer
        consumer = KafkaConsumer(
            KAKFA_TOPICS[1],
            bootstrap_servers=['broker:29092'],
            auto_offset_reset='earliest',
            enable_auto_commit=False,  # Manually commit offsets
            group_id='my-group'
        )

        # Set up a session to connect to cassandra
        cluster = Cluster(['cassandra'])
        session = cluster.connect().set_keyspace(KEYSPACE)

      
        # Poll for messages for a specified time
        end_time = time.time() + DURATION

        while time.time() < end_time:

            # Poll with a 5-second timeout
            messages = consumer.poll(timeout_ms=5000, max_records= 2) 

            if messages:
                for topic_partition, messages in messages.items():
                    for message in messages:

                        # Decode the Kafka message
                        data = json.loads(message.value.decode('utf-8'))
                        print(f"Consumed message and topic: {message.topic} with value: {data}")

                        # Insert the data into Cassandra
                        session.execute("""
                        INSERT INTO weather.general (id, updated_Timestamp, low_temp, high_temp, low_humidity, high_humidity, general_forecast, low_wind_speed, high_wind_speed, wind_direction)
                        VALUES (uuid(), %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (data['updated_Timestamp'], data['low_temp'], data['high_temp'], data['low_humidity'], data['high_humidity'], data['general_forecast'], data['low_wind_speed'], data['high_wind_speed'], data['wind_direction'],))

                # Manually commit the Kafka offset to avoid reprocessing
                consumer.commit()
            else:
                logging.info("No messages consumed within the polling period.")

        logging.info("Time limit reached. Stopping consumer...")
        
        consumer.close()
        session.shutdown()



    @task_group(group_id="produce_weather_data")
    def produce_weather_data(general_data: dict, periodic_data: list):
        produce_periodic_weather_data(periodic_data)
        produce_general_weather_data(general_data)
    
    @task_group(group_id="consume_weather_data")
    def consume_weather_data():
        data_sensor() >> [consume_periodic_weather_data(), consume_general_weather_data()]


    create_cassandra_keyspace_table()

    payload = extract_payload()
    general_data = extract_general_data(payload)
    periodic_data = extract_periodic_data(payload)

    produce_weather_data(general_data, periodic_data)
    consume_weather_data()

weather_analysis_dag()
