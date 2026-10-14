# 🔌 Kafka Connect

---

## What is Kafka Connect?

A framework for **connecting Kafka with external systems** (databases, file systems, search engines, cloud storage) without writing custom code.

```
  ┌───────────────────────────────────────────────────────────────────┐
  │                     KAFKA CONNECT ECOSYSTEM                       │
  │                                                                   │
  │  External Systems          Kafka               External Systems   │
  │                                                                   │
  │  MySQL ──────────────┐                  ┌──────── Elasticsearch  │
  │  PostgreSQL ─────────┤                  ├──────── S3 / GCS        │
  │  MongoDB ────────────┼─▶ SOURCE      SINK ├──────── HDFS           │
  │  Files ──────────────┤  CONNECTOR   CONNECTOR├──── Snowflake       │
  │  REST API ───────────┘     │           │   └──────── Redis         │
  │                            ▼           ▲                          │
  │                       ┌────────────────────┐                      │
  │                       │    KAFKA TOPICS    │                      │
  │                       └────────────────────┘                      │
  └───────────────────────────────────────────────────────────────────┘
```

---

## Source vs Sink Connectors

| Type | Direction | Example |
|------|-----------|---------|
| **Source** | External → Kafka | MySQL CDC → Kafka |
| **Sink** | Kafka → External | Kafka → Elasticsearch |

---

## Popular Connectors

| Connector | Type | Use Case |
|-----------|------|---------|
| Debezium MySQL | Source | CDC from MySQL |
| JDBC Source | Source | Database polling |
| S3 Sink | Sink | Archive to S3 |
| Elasticsearch Sink | Sink | Search indexing |
| HDFS Sink | Sink | Big data storage |
| HTTP Source | Source | REST API ingestion |

---

## Example: MySQL CDC with Debezium

```json
{
  "name": "mysql-cdc-connector",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "database.hostname": "localhost",
    "database.port": "3306",
    "database.user": "kafka_user",
    "database.password": "password",
    "database.server.name": "myserver",
    "table.include.list": "mydb.orders",
    "database.history.kafka.topic": "schema-changes.mydb"
  }
}
```

Every INSERT/UPDATE/DELETE in MySQL → automatically published to Kafka!
