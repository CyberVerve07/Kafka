# 🖥️ Kafka Brokers and Clusters

---

## What is a Broker?

A **broker** is a single Kafka server. It:
- Receives records from producers
- Stores records on disk
- Serves records to consumers
- Manages partition leadership

---

## 🏗️ Cluster Architecture

```
  ┌─────────────────────────────────────────────────────────────────────────┐
  │                          KAFKA CLUSTER                                  │
  │                                                                         │
  │  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐      │
  │  │    BROKER 1      │  │    BROKER 2      │  │    BROKER 3      │      │
  │  │  (Bootstrap)     │  │                  │  │                  │      │
  │  │                  │  │                  │  │                  │      │
  │  │ Topic: orders    │  │ Topic: orders    │  │ Topic: orders    │      │
  │  │ P0 ★ Leader      │  │ P1 ★ Leader      │  │ P2 ★ Leader      │      │
  │  │ P2   Replica     │  │ P0   Replica     │  │ P1   Replica     │      │
  │  │                  │  │                  │  │                  │      │
  │  │ Topic: users     │  │ Topic: users     │  │ Topic: users     │      │
  │  │ P0 ★ Leader      │  │ P1   Replica     │  │ P1 ★ Leader      │      │
  │  └──────────────────┘  └──────────────────┘  └──────────────────┘      │
  │           │                      │                     │               │
  │           └──────────────────────┴─────────────────────┘               │
  │                                  │                                      │
  │                         ┌────────▼────────┐                            │
  │                         │  ZOOKEEPER /    │                            │
  │                         │    KRaft        │                            │
  │                         │  Controller     │                            │
  │                         └─────────────────┘                            │
  └─────────────────────────────────────────────────────────────────────────┘
```

---

## 🌟 Broker Roles

### Controller Broker
One broker is elected as **Controller**. It is responsible for:
- Partition leader election
- Monitoring broker liveness
- Reassigning partitions on broker failure

```
  Broker 1 (Controller) ──── monitors ────▶ Broker 2, Broker 3
  
  Broker 2 crashes!
  ↓
  Controller detects via Zookeeper/KRaft
  ↓
  Elects new leader from ISR for Broker 2's leader partitions
  ↓
  Updates metadata on all brokers
  ↓
  Clients redirect to new leader
```

---

## 📡 Client-Broker Communication

```
  Producer startup:
  1. Connect to any bootstrap broker
  2. Broker returns cluster METADATA (all brokers, topics, leaders)
  3. Producer caches metadata locally
  4. Producer connects directly to partition LEADERS

  Producer ──── metadata request ────▶ bootstrap broker
  Producer ◀─── cluster metadata ─────
  Producer ──── produce request ─────▶ correct leader broker
```

---

## 🔢 Broker Configuration Essentials

| Config | Typical Value | Description |
|--------|--------------|-------------|
| `num.partitions` | 3 | Default partition count |
| `log.retention.hours` | 168 (7 days) | How long to keep logs |
| `log.segment.bytes` | 1073741824 (1GB) | Max segment file size |
| `log.retention.bytes` | -1 (unlimited) | Max partition size |
| `min.insync.replicas` | 2 | Min ISR for acks=all |
| `default.replication.factor` | 3 | Default replication |
| `auto.create.topics.enable` | false | Disable in production |
| `offsets.retention.minutes` | 10080 | Consumer offset retention |
