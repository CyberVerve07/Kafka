# ⚙️ Setup & Configuration

## Docker Compose — Start Kafka Locally

Run this to get Kafka + Zookeeper running in seconds:

```bash
docker-compose up -d
```

Check topics:
```bash
docker exec -it kafka kafka-topics.sh --list --bootstrap-server localhost:9092
```
