from confluent_kafka import DeserializingConsumer
from models import ride_deserializer

topic_name = "rides"

consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "ride_consumer_group",
    "auto.offset.reset": "earliest",
    "value.deserializer": ride_deserializer
}

consumer = DeserializingConsumer(consumer_config)
consumer.subscribe([topic_name])

print(f"Listening to {topic_name}...")



## CASE 1: One Record
## -----------------------------------------------------------------------
try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        ride = msg.value()

        #  PULocationID=int(row['PULocationID']),
        #         DOLocationID=int(row['DOLocationID']),
        #         trip_distance=float(row['trip_distance']),
        #         total_amount=float(row['total_amount']),
        #         tpep_pickup_datetime=int(row['tpep_pickup_datetime'].timestamp() * 1000),

        print(f"Pickup Location ID: {ride.PULocationID}, Dropoff Location ID: {ride.DOLocationID}, Trip Distance: {ride.trip_distance}, Total Amount: {ride.total_amount}, Pickup Datetime (epoch ms): {ride.tpep_pickup_datetime}")

except KeyboardInterrupt:
    print("Consumer interrupted by user. Closing consumer...")

finally:
    consumer.close()


## CASE 2: Multiple Records
## -----------------------------------------------------------------------
try:
    while True:
        for message in consumer:
            if message is None:
                continue
            if message.error():
                print(f"Consumer error: {message.error()}")
                continue

            ride = message.value()
        #  PULocationID=int(row['PULocationID']),
        #         DOLocationID=int(row['DOLocationID']),
        #         trip_distance=float(row['trip_distance']),
        #         total_amount=float(row['total_amount']),
        #         tpep_pickup_datetime=int(row['tpep_pickup_datetime'].timestamp() * 1000),

            print(f"Pickup Location ID: {ride.PULocationID}, Dropoff Location ID: {ride.DOLocationID}, Trip Distance: {ride.trip_distance}, Total Amount: {ride.total_amount}, Pickup Datetime (epoch ms): {ride.tpep_pickup_datetime}")

except KeyboardInterrupt:
    print("Consumer interrupted by user. Closing consumer...")

finally:
    consumer.close()