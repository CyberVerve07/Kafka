# 📚 Kafka Architecture — Deep Dive

> Complete guide to understanding Apache Kafka's internal architecture, components, and design principles.

---

## 📁 Folder Structure

```
01-Kafka-Architecture/
│
├── 01-Introduction/
│   └── README.md          ← What is Kafka? Why use it?
│
├── 02-Core-Concepts/
│   └── README.md          ← Topics, Partitions, Offsets, Messages
│
├── 03-Producers/
│   └── README.md          ← Producer internals, acks, batching
│
├── 04-Consumers/
│   └── README.md          ← Consumer groups, offset management
│
├── 05-Topics-and-Partitions/
│   └── README.md          ← Partition strategy, ordering guarantees
│
├── 06-Brokers-and-Clusters/
│   └── README.md          ← Broker roles, leader election
│
├── 07-Zookeeper-and-KRaft/
│   └── README.md          ← Coordination layer evolution
│
├── 08-Replication-and-Fault-Tolerance/
│   └── README.md          ← ISR, replication factor, failover
│
├── 09-Kafka-Streams/
│   └── README.md          ← Stream processing primitives
│
└── 10-Kafka-Connect/
    └── README.md          ← Source & sink connectors
```

---

## 🏗️ Full Kafka Architecture Diagram

```
                          ╔══════════════════════════════════════════════════╗
                          ║           APACHE KAFKA ARCHITECTURE              ║
                          ╚══════════════════════════════════════════════════╝

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                            PRODUCER SIDE                                    │
  │                                                                             │
  │  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐                    │
  │  │  Application │   │  Application │   │  Microservice│                    │
  │  │   (Producer) │   │   (Producer) │   │  (Producer)  │                    │
  │  └──────┬───────┘   └──────┬───────┘   └──────┬───────┘                    │
  │         │                  │                  │                             │
  │         └──────────────────┴──────────────────┘                             │
  │                            │                                                │
  │                   ┌────────▼─────────┐                                      │
  │                   │  Partitioner     │  ← Round Robin / Key-based           │
  │                   │  Record Batcher  │  ← linger.ms, batch.size             │
  │                   │  Compressor      │  ← gzip, snappy, lz4                 │
  │                   └────────┬─────────┘                                      │
  └────────────────────────────┼────────────────────────────────────────────────┘
                               │
  ┌────────────────────────────▼────────────────────────────────────────────────┐
  │                         KAFKA CLUSTER                                       │
  │                                                                             │
  │   ┌─────────────────────────────────────────────────────────────────────┐  │
  │   │                    BROKER 1 (Leader for P0, P2)                      │  │
  │   │   ┌─────────────────────────────────────────────────────────────┐   │  │
  │   │   │  Topic: "orders"                                            │   │  │
  │   │   │  ┌─────────────────┐  ┌─────────────────┐                  │   │  │
  │   │   │  │  Partition 0    │  │  Partition 2    │                  │   │  │
  │   │   │  │  [msg0][msg1].. │  │  [msg0][msg1].. │                  │   │  │
  │   │   │  │  offset: 0,1,2  │  │  offset: 0,1,2  │                  │   │  │
  │   │   │  └─────────────────┘  └─────────────────┘                  │   │  │
  │   │   └─────────────────────────────────────────────────────────────┘   │  │
  │   └─────────────────────────────────────────────────────────────────────┘  │
  │                                                                             │
  │   ┌─────────────────────────────────────────────────────────────────────┐  │
  │   │                    BROKER 2 (Leader for P1)                          │  │
  │   │   ┌─────────────────────────────────────────────────────────────┐   │  │
  │   │   │  Topic: "orders"                                            │   │  │
  │   │   │  ┌─────────────────┐                                        │   │  │
  │   │   │  │  Partition 1    │  ← Also holds replicas of P0, P2       │   │  │
  │   │   │  │  [msg0][msg1].. │                                        │   │  │
  │   │   │  └─────────────────┘                                        │   │  │
  │   │   └─────────────────────────────────────────────────────────────┘   │  │
  │   └─────────────────────────────────────────────────────────────────────┘  │
  │                                                                             │
  │   ┌──────────────────────────────────────────────────────────────────────┐ │
  │   │  ZOOKEEPER / KRaft                                                   │ │
  │   │  • Broker registration & discovery                                   │ │
  │   │  • Leader election                                                   │ │
  │   │  • Topic & partition metadata                                        │ │
  │   │  • ACLs and quotas                                                   │ │
  │   └──────────────────────────────────────────────────────────────────────┘ │
  └────────────────────────────┬────────────────────────────────────────────────┘
                               │
  ┌────────────────────────────▼────────────────────────────────────────────────┐
  │                          CONSUMER SIDE                                      │
  │                                                                             │
  │   ┌──────────────────────────────────────────────────────────────────────┐ │
  │   │  Consumer Group: "order-processing-service"                          │ │
  │   │                                                                      │ │
  │   │   Consumer-1          Consumer-2          Consumer-3                 │ │
  │   │   reads P0            reads P1            reads P2                   │ │
  │   │   offset: 5           offset: 3           offset: 7                  │ │
  │   │                                                                      │ │
  │   │   ← Group Coordinator manages rebalancing →                          │ │
  │   └──────────────────────────────────────────────────────────────────────┘ │
  │                                                                             │
  │   ┌──────────────────────────────────────────────────────────────────────┐ │
  │   │  Consumer Group: "analytics-service"  (independent offset tracking)  │ │
  │   │   Consumer-A reads ALL partitions (P0, P1, P2) independently        │ │
  │   └──────────────────────────────────────────────────────────────────────┘ │
  └─────────────────────────────────────────────────────────────────────────────┘
```

---

## 🔑 Core Concepts At a Glance

| Concept | Description |
|---------|-------------|
| **Topic** | Logical channel to which records are published |
| **Partition** | Ordered, immutable log; unit of parallelism |
| **Offset** | Unique ID of a record within a partition |
| **Producer** | Publishes records to topics |
| **Consumer** | Reads records from topics |
| **Consumer Group** | Logical grouping for parallel consumption |
| **Broker** | Kafka server node in the cluster |
| **Leader** | Broker responsible for reads/writes for a partition |
| **Replica** | Copy of a partition on a different broker |
| **ISR** | In-Sync Replicas — replicas caught up with the leader |
| **Zookeeper/KRaft** | Coordination and metadata management |

---

## ⚡ Message Lifecycle

```
Producer                    Kafka Broker                    Consumer
   │                             │                              │
   │──── send(record) ──────────▶│                              │
   │                        Append to                           │
   │                        Partition Log                       │
   │                             │──── replicate ──────────────▶│ (replicas)
   │◀─── ack (based on acks cfg) ─┤                              │
   │                             │                              │
   │                             │◀─── fetch(offset) ───────────│
   │                             │──── records ────────────────▶│
   │                             │                              │──── process()
   │                             │◀─── commit offset ───────────│
```

---

## 📂 Navigate Each Section

| # | Section | What You'll Learn |
|---|---------|-------------------|
| 01 | [Introduction](./01-Introduction/README.md) | Kafka origin, use cases, when to use |
| 02 | [Core Concepts](./02-Core-Concepts/README.md) | Topics, partitions, offsets |
| 03 | [Producers](./03-Producers/README.md) | Producer configs, acks, batching |
| 04 | [Consumers](./04-Consumers/README.md) | Consumer groups, rebalancing |
| 05 | [Topics & Partitions](./05-Topics-and-Partitions/README.md) | Partition strategy & ordering |
| 06 | [Brokers & Clusters](./06-Brokers-and-Clusters/README.md) | Cluster management |
| 07 | [Zookeeper & KRaft](./07-Zookeeper-and-KRaft/README.md) | Coordination evolution |
| 08 | [Replication](./08-Replication-and-Fault-Tolerance/README.md) | Fault tolerance & ISR |
| 09 | [Kafka Streams](./09-Kafka-Streams/README.md) | Real-time stream processing |
| 10 | [Kafka Connect](./10-Kafka-Connect/README.md) | Data pipeline connectors |
