# 📖 Introduction to Apache Kafka

## What is Kafka?

Apache Kafka is an **open-source distributed event streaming platform** originally developed by LinkedIn and donated to the Apache Software Foundation. It is designed to handle **high-throughput, fault-tolerant, real-time data feeds**.

> Kafka treats each event as an immutable record in a **distributed commit log**.

---

## 🧠 Why Was Kafka Built?

LinkedIn had a problem in ~2010:
- Millions of user activity events per day
- Multiple systems needed the same data (analytics, recommendations, monitoring)
- Point-to-point pipelines were becoming a web of complexity

```
  BEFORE KAFKA (Spaghetti Architecture):
  
  App1 ──────────────────▶ Analytics DB
  App1 ──────▶ Search Index
  App1 ──────────────────────────────▶ Monitor
  App2 ──────────────▶ Analytics DB
  App2 ──────────────────▶ Search Index
  
  AFTER KAFKA (Hub & Spoke):
  
  App1 ─────┐              ┌──▶ Analytics DB
  App2 ─────┼──▶ KAFKA ───┼──▶ Search Index
  App3 ─────┘              └──▶ Monitor
```

---

## 🌍 Real-World Use Cases

| Use Case | Example |
|----------|---------|
| **Activity Tracking** | User clicks, page views, purchases |
| **Metrics & Monitoring** | Application metrics, server health |
| **Log Aggregation** | Centralize logs from multiple services |
| **Stream Processing** | Fraud detection, real-time recommendations |
| **Event Sourcing** | Order management, inventory updates |
| **Microservice Messaging** | Async communication between services |
| **CDC** | Change Data Capture from databases |

---

## ⚡ Kafka vs Traditional Messaging

| Feature | Kafka | RabbitMQ | ActiveMQ |
|---------|-------|----------|---------|
| **Message Retention** | Days/weeks | Until consumed | Until consumed |
| **Throughput** | Millions/sec | ~50K/sec | ~10K/sec |
| **Message Replay** | ✅ Yes | ❌ No | ❌ No |
| **Ordering** | Per partition | Per queue | Per queue |
| **Consumer Groups** | ✅ Yes | ✅ Yes | ✅ Yes |
| **Pull Model** | ✅ Pull | Both | Both |
| **Best For** | Event streaming | Task queues | Enterprise MQ |

---

## 🏆 Companies Using Kafka

- **LinkedIn** — 7 trillion messages/day
- **Netflix** — 700 billion+ events/day
- **Uber** — Real-time pricing, ETA
- **Airbnb** — Search indexing
- **Goldman Sachs** — Financial transactions
- **Twitter** — Activity streams
