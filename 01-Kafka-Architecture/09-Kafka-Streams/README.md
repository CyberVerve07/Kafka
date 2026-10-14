# 🌊 Kafka Streams

---

## What is Kafka Streams?

A **client library** for building real-time stream processing applications on top of Kafka. No separate cluster needed — it runs inside your application.

```
  ┌─────────────────────────────────────────────────────────────┐
  │                  KAFKA STREAMS TOPOLOGY                     │
  │                                                             │
  │  Source Topic        Processing          Sink Topic         │
  │  "orders"            (your app)          "order-stats"      │
  │                                                             │
  │  [KStream]──▶ filter ──▶ map ──▶ groupBy ──▶ count ──▶ [KTable]│
  │                                                             │
  │  KStream: unbounded stream of events                        │
  │  KTable:  changelog stream (latest value per key)           │
  └─────────────────────────────────────────────────────────────┘
```

---

## KStream vs KTable

| | KStream | KTable |
|-|---------|--------|
| **Nature** | Event stream (each record = new event) | Update stream (each record = latest state) |
| **Analogy** | INSERT into table | UPSERT into table |
| **Use case** | Clicks, payments, orders | User profiles, product prices |
| **Window** | Windowed aggregations | Materialized views |

---

## Stream Processing Operations

```java
  StreamsBuilder builder = new StreamsBuilder();
  
  // 1. Source
  KStream<String, Order> orders = builder.stream("orders");
  
  // 2. Filter — only paid orders
  KStream<String, Order> paid = orders.filter(
      (key, order) -> order.getStatus().equals("PAID")
  );
  
  // 3. Map — transform to summary
  KStream<String, String> summaries = paid.mapValues(
      order -> "Order " + order.getId() + " = $" + order.getAmount()
  );
  
  // 4. Sink — write to output topic
  summaries.to("order-summaries");
  
  // 5. Aggregation — count by userId (5-min window)
  KTable<Windowed<String>, Long> counts = orders
      .groupByKey()
      .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
      .count();
```
