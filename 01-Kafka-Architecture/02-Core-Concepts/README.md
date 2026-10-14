# 🔑 Kafka Core Concepts

## The Fundamental Building Blocks

---

## 1. 📨 Message (Record / Event)

The basic unit of data in Kafka. A record contains:

```
┌─────────────────────────────────────────────────────┐
│                  KAFKA RECORD                       │
│                                                     │
│  ┌──────────┬─────────────┬──────────────────────┐  │
│  │  Headers │     Key     │        Value         │  │
│  │ (opt.)   │  (opt.)     │    (the payload)     │  │
│  └──────────┴─────────────┴──────────────────────┘  │
│                                                     │
│  + Timestamp                                        │
│  + Partition                                        │
│  + Offset                                           │
└─────────────────────────────────────────────────────┘
```

- **Key**: Used for partitioning (same key → same partition)
- **Value**: The actual data (JSON, Avro, Protobuf, etc.)
- **Timestamp**: Event time or ingestion time
- **Headers**: Metadata like correlation IDs, trace IDs

---

## 2. 📂 Topic

A **logical category** that acts like a named channel. Think of it like a database table name.

```
  Topic: "user-events"
  ┌──────────────────────────────────────────────────┐
  │  Partition 0: [e1][e2][e3][e4]────▶ (new events) │
  │  Partition 1: [e1][e2][e3]────────▶              │
  │  Partition 2: [e1][e2][e3][e4][e5]▶              │
  └──────────────────────────────────────────────────┘
```

- Topics are **append-only** — records are never updated in place
- Topics are **durable** — retention is configurable (default: 7 days)
- Topics can be **compacted** — keep only the latest value per key

---

## 3. 📊 Partition

A partition is an **ordered, immutable sequence** of records. It is the unit of parallelism.

```
  Partition 0:
  ┌────┬────┬────┬────┬────┬────┐
  │ 0  │ 1  │ 2  │ 3  │ 4  │ 5  │──▶ new records appended here
  └────┴────┴────┴────┴────┴────┘
  offset

  Rules:
  ✅ Ordering is guaranteed WITHIN a partition
  ❌ Ordering is NOT guaranteed ACROSS partitions
```

---

## 4. 🔢 Offset

A **unique sequential ID** for each record within a partition:

```
  Partition 0:
  [offset=0][offset=1][offset=2][offset=3]──▶

  Consumer A is at offset 2 → has read records 0,1,2
  Consumer B is at offset 0 → hasn't read anything yet

  Both are INDEPENDENT — Kafka doesn't delete on consume!
```

---

## 5. Key Relationships

```
  KAFKA
  └── Cluster
      ├── Broker 1
      │   └── Topic: "orders"
      │       ├── Partition 0 (Leader here)
      │       │   ├── Segment file 1
      │       │   └── Segment file 2
      │       └── Partition 2 (Replica here)
      └── Broker 2
          └── Topic: "orders"
              ├── Partition 1 (Leader here)
              └── Partition 0 (Replica here)
```

---

## 📝 Quick Reference

| Term | Definition |
|------|-----------|
| Record/Message | The actual data unit |
| Topic | Named log for a category of records |
| Partition | Ordered sub-log within a topic |
| Offset | Record's position within a partition |
| Segment | Physical file storing a partition's records |
| Log | The complete partition (all segments) |
