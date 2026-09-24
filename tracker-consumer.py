from confluent_kafka import Consumer
import json

consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "orders-tracker",
    "auto.offset.reset": "earliest"
}

consumer = Consumer(consumer_config)

consumer.subscribe(["orders"])
print("This consumer is subscribed to the topic: orders")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print(f"There was some error: {msg.error()}")
            continue

        ## Read the message
        value = msg.value().decode('utf-8')
        order = json.loads(value)

        print(f"Recieved new order for {order['product']} x  {order['quantity']} placed by {order['user']}")

except KeyboardInterrupt:
    print("Closed Tracker Consumer")
finally: 
    consumer.close()