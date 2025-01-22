import requests
from quixstreams import Application


app = Application(broker_address="localhost:9092", consumer_group="weather_data")
general_weather_topic = app.topic(name="general", value_serializer="json")
periodic_weather_topic = app.topic(name="periodic", value_serializer="json") 

# Get the data from the API
def extract_weather_data():
    
    url = "https://api-open.data.gov.sg/v2/real-time/api/twenty-four-hr-forecast"
    response = requests.get(url)

    # if the response is successful, start extracting the data
    if response.status_code == 200:
        
        # access the content of the response
        content = response.json()    

        # extract the data from the content
        data_records = content['data']['records'][0]

        # further extraction into general and periodic data
        general_data, periodic_data = data_records["general"], data_records["periods"]

        # Extract the general data
        updated_Timestamp = data_records["updatedTimestamp"]

        temperatures = general_data["temperature"]
        low_temp, high_temp = temperatures["low"], temperatures["high"]

        humidity = general_data["relativeHumidity"]
        low_humidity, high_humidity = humidity["low"], humidity["high"]

        general_forecast = general_data["forecast"]["text"]

        wind = general_data["wind"]
        low_wind_speed, high_wind_speed = wind["speed"]["low"], wind["speed"]["high"]

        wind_direction = wind["direction"]

        general_data = {
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


        
        return general_data, periodic_datas



def Produce_to_Kafka(general_data: dict, periodic_data: list):

    with app.get_producer() as producer:
        
        # Serialize the general data and send it to Kafka
        general_kafka_message = general_weather_topic.serialize(key=general_data["updated_Timestamp"], value=general_data)

        print(f'Produce event with key="{general_kafka_message.key}" value="{general_kafka_message.value}"')
        
        # Produce the message to the Kafka topic
        producer.produce(
            topic = general_weather_topic.name, 
            key   = general_kafka_message.key, 
            value = general_kafka_message.value
        )

        for data in periodic_data:
            
            # Serialize each of the periodic data and send it to Kafka
            periodic_kafka_message = periodic_weather_topic.serialize(key=data["start_time_period"], value=data)
            
            print(f'Produce event with key="{periodic_kafka_message.key}" value="{periodic_kafka_message.value}"')
            
            # Serialize each of the periodic data and send it to Kafka
            producer.produce(
                topic = periodic_weather_topic.name,
                key   = periodic_kafka_message.key, 
                value = periodic_kafka_message.value
            )


general_data, periodic_data = extract_weather_data()
Produce_to_Kafka(general_data, periodic_data)