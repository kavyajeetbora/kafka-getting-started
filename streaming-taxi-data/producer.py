from confluent_kafka import SerializingProducer
import pandas as pd
from models import ride_serializer, ride_deserializer, ride_from_row
from time import time

## Create Kafka Producer
producer_config = {
        "bootstrap.servers": "localhost:9092",
        "value.serializer": ride_serializer
    }
producer = SerializingProducer(producer_config)

url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet"
columns = ['PULocationID', 'DOLocationID', 'trip_distance', 'total_amount', 'tpep_pickup_datetime']
df = pd.read_parquet(url, columns=columns).head(1000)


## CASE 1: One Record
## -----------------------------------------------------------------------
## Send the data from producer to kafka topic 'rides'
# t0 = time()
# topic_name = "rides"

# row = df.iloc[0]
# data = ride_from_row(row) ## This will return a Ride object with defined schema

# producer.produce(topic=topic_name, value=data)

# producer.flush() ## if in any event the producer is not able to send the message, it will wait for the message to be sent before exiting the program

# t1 = time()

# print(f"Sent 1 message in {t1-t0} seconds")


## CASE 2: Multiple Records
## -----------------------------------------------------------------------

t0 = time()
topic_name = "rides"

try:

    for index,row in df.iterrows():

        data = ride_from_row(row) ## This will return a Ride object with defined schema
        producer.produce(topic=topic_name, value=data)
except Exception as e:
    print(f"Exception occured while sending data to kafka topic: {e}")

finally:
    producer.flush() ## if in any event the producer is not able to send the message, it will wait for the message to be sent before exiting the program

t1 = time()

print(f"Sent {len(df)} messages in {t1-t0} seconds")