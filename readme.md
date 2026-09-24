## Video Tutorial

[![Watch the video](https://img.youtube.com/vi/B7CwU_tNYIE/maxresdefault.jpg)](https://youtu.be/B7CwU_tNYIE)

## Debug Kakfa using CLI

```bash
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
```

Check the topics
```bash
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic orders
```

Check the events

```sh
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic orders --from-beginning
```

## Reference 
1. [cli kafka-tools](https://docs.confluent.io/kafka/operations-tools/kafka-tools.html)