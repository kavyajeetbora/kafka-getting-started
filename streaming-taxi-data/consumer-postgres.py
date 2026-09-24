import psycopg2
from confluent_kafka import DeserializingConsumer
from models import ride_deserializer

## Connect to Postgres
server = 'localhost:5432'
topic_name = 'rides'

conn = psycopg2.connect(
    host='localhost',
    port=5432,
    database='taxi_rides',
    user='postgres',
    password='postgres'
)
conn.autocommit = True
cur = conn.cursor()

## Check connection to Postgres
try:
    cur.execute("SELECT version();")
    db_version = cur.fetchone()
    print(f"Connected to Postgres database. Version: {db_version[0]}")
except Exception as e:
    print(f"Error connecting to Postgres database: {e}")
    exit(1)

## Create a table in Postgres if it doesn't exist
create_table_query = f"""
CREATE TABLE IF NOT EXISTS {topic_name} (
    PULocationID INT,
    DOLocationID INT,
    trip_distance FLOAT,
    total_amount FLOAT,
    tpep_pickup_datetime BIGINT
);
"""

try: 
    cur.execute(create_table_query)
    print(f"Table '{topic_name}' created successfully in Postgres.")
except Exception as e:
    print(f"Error creating table in Postgres: {e}")
    exit(1)

## Ingest data from Kafka topic to Postgres
## -----------------------------------------------------------------------

## Create the consumer configuration
consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "ride_postgres_consumer_group",
    "auto.offset.reset": "earliest",
    "value.deserializer": ride_deserializer
}

consumer = DeserializingConsumer(consumer_config)
topic_name = "rides"
consumer.subscribe([topic_name])

print(f"Listening to Kafka topic:{topic_name} and ingesting data into Postgres...")
try:
    while True:
        message = consumer.poll(1.0)
        if message is None:
            continue
        if message.error():
            print(f"Consumer error: {message.error()}")
            continue

        ride = message.value()
        query = f"""
            INSERT INTO {topic_name} (PULocationID, DOLocationID, trip_distance, total_amount, tpep_pickup_datetime) 
            VALUES (%s, %s, %s, %s, %s);
        """

        cur.execute(query, (ride.PULocationID, ride.DOLocationID, ride.trip_distance, ride.total_amount, ride.tpep_pickup_datetime))
        print(f"Inserted ride into Postgres: Pickup Location ID: {ride.PULocationID}, Dropoff Location ID: {ride.DOLocationID}, Trip Distance: {ride.trip_distance}, Total Amount: {ride.total_amount}, Pickup Datetime (epoch ms): {ride.tpep_pickup_datetime}")

except KeyboardInterrupt:
    print("Consumer interrupted by user. Closing consumer...")

finally:
    consumer.close()
    cur.close()
    conn.close()