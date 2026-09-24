from confluent_kafka import Producer
import json
import uuid
from faker import Faker
fake = Faker(provider='commerce')

## Setting up kafka connection for the producer at listening port
producer_config = {
    "bootstrap.servers" : 'localhost:9092'
}

producer = Producer(producer_config)

PRODUCTS = ["Coca-Cola 500ml", "Sprite 1L", "Thums Up 750ml", "Maaza 600ml", "Limca 300ml"]

## Define the data for the message: order
order = {
    "order_id": str(uuid.uuid4()),
    "user": fake.name(),
    "product": fake.random_element(PRODUCTS),
    "price": fake.pyfloat(min_value=10, max_value=500, right_digits=2),
    "quantity": fake.random_int(min=1, max=10)
}
## Convert the message to JSON format and encode it to bytes for kafka
value = json.dumps(order).encode('utf-8')

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Delivered {msg.value().decode('utf-8')}")
        print(f"✅ Delivered to topic: '{msg.topic()}' : partition {msg.partition()} : at offset {msg.offset()}")


## Send the message to the kafka topic 'orders' and specify the callback function to handle delivery reports
producer.produce(
    topic='orders',
    value=value,
    callback=delivery_report
)

producer.flush() ## if in any event the producer is not able to send the message, it will wait for the message to be sent before exiting the program