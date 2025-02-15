import requests, json, time, logging, sys, six

from airflow.decorators import dag, task, task_group
from airflow.sensors.base import PokeReturnValue
from airflow.models import Variable
from kafka import KafkaConsumer
from datetime import datetime, timedelta
from cassandra.cluster import Cluster

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
@dag(dag_id='weather_consumer_dag',
     schedule_interval='*/5 * * * *',
     default_args=default_args,
     catchup=False,
     tags=['Loading'])
def consumer_dag():

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
        session = cluster.connect()
        
        session.set_keyspace(KEYSPACE)

      
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
        session = cluster.connect()
        
        session.set_keyspace(KEYSPACE)
      
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
                        """, 
                        (   str(data['updated_Timestamp']), 
                            str(data['low_temp']), 
                            str(data['high_temp']),
                            str(data['low_humidity']), 
                            str(data['high_humidity']), 
                            str(data['general_forecast']), 
                            str(data['low_wind_speed']), 
                            str(data['high_wind_speed']), 
                            str(data['wind_direction']),)
                        )

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
    consume_weather_data()

consumer_dag()
