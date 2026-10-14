# 🔀 Topics and Partitions — Strategy Guide

---

## Topics

A **topic** is an append-only distributed log with configurable retention and replication:

```
  kafka-topics.sh --create \
    --topic orders \
    --partitions 6 \
    --replication-factor 3 \
    --config retention.ms=604800000   ← 7 days
```

---

## Partitions — The Unit of Parallelism

```
  Topic: "orders" with 4 partitions across 3 brokers:
  
  ┌──────────────────────────────────────────────────────────────┐
  │  Broker 1                                                    │
  │  ├── Partition 0 (LEADER)   [0][1][2][3][4]──▶              │
  │  └── Partition 2 (REPLICA)  [0][1][2]──▶                    │
  │                                                              │
  │  Broker 2                                                    │
  │  ├── Partition 1 (LEADER)   [0][1][2][3]──▶                 │
  │  └── Partition 3 (REPLICA)  [0][1]──▶                       │
  │                                                              │
  │  Broker 3                                                    │
  │  ├── Partition 2 (LEADER)   [0][1][2][3]──▶                 │
  │  └── Partition 0 (REPLICA)  [0][1][2][3][4]──▶              │
  └──────────────────────────────────────────────────────────────┘
```

---

## 🔑 How Records Are Routed to Partitions

```
  Record with KEY:
  hash(key) % numPartitions = target partition
  
  Example: key="user-123"
  hash("user-123") = 458976
  458976 % 4 = 0  → always goes to Partition 0
  
  ✅ Guarantees: all records with same key are ordered!

  Record WITHOUT KEY:
  → Round Robin across partitions (or Sticky Partitioner)
```

---

## 📐 How to Choose Partition Count

```
  Considerations:
  ┌─────────────────────────────────────────────────────────────┐
  │  • Target throughput ÷ throughput per partition             │
  │  • Number of consumers in your largest group                │
  │  • Future scalability needs                                 │
  │                                                             │
  │  Rule of thumb:                                             │
  │  partitions = max(T_producer/T_partition, T_consumer/T_p)   │
  │                                                             │
  │  For most production topics: 12-100 partitions              │
  └─────────────────────────────────────────────────────────────┘
  
  ⚠️  You CAN increase partitions later, but:
      - Ordering guarantee per key breaks
      - Rebalancing occurs
      You CANNOT decrease partitions!
```

---

## 🗜️ Log Compaction

For stateful data (e.g., user profiles), use **log compaction** instead of time-based retention:

```
  BEFORE compaction (Topic: user-profiles, key=userId):
  [u1:v1][u2:v1][u1:v2][u3:v1][u2:v2][u1:v3]

  AFTER compaction (keeps only latest per key):
  [u3:v1][u2:v2][u1:v3]

  Perfect for: Change Data Capture, event sourcing snapshots
```

---

## 📊 Partition Count vs Consumer Count

| Partitions | Consumers | Result |
|-----------|-----------|--------|
| 6 | 2 | Each consumer reads 3 partitions |
| 6 | 6 | Perfect 1:1 mapping ✅ |
| 6 | 8 | 2 consumers idle ⚠️ |
| 6 | 1 | Single consumer reads all (no parallelism) |
