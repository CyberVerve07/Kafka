# 🔄 Zookeeper vs KRaft — Coordination Layer

---

## Zookeeper (Legacy — Before Kafka 3.x)

```
  ┌──────────────────────────────────────────────────────────────┐
  │                    ZOOKEEPER ENSEMBLE                        │
  │                                                              │
  │  ┌──────────┐  ┌──────────┐  ┌──────────┐                   │
  │  │  ZK-1    │  │  ZK-2    │  │  ZK-3    │                   │
  │  │ (Leader) │──│(Follower)│──│(Follower)│                   │
  │  └──────────┘  └──────────┘  └──────────┘                   │
  │        │                                                     │
  │        ▼  Stores:                                            │
  │  /brokers/ids/[1,2,3]     ← Active brokers                  │
  │  /brokers/topics/orders   ← Topic metadata                  │
  │  /controller              ← Who is the controller            │
  │  /consumers/[group-id]    ← Old consumer group info         │
  └──────────────────────────────────────────────────────────────┘
              ▲                   ▲
       Kafka Brokers          Kafka Brokers
       register here         watch for changes
```

**Problems with Zookeeper:**
- Separate system to operate and monitor
- Limits Kafka to ~200,000 partitions
- Controller failover requires full metadata reload
- Operational complexity

---

## KRaft (Kafka 3.3+ — Zookeeper Removed!)

```
  ┌──────────────────────────────────────────────────────────────┐
  │                   KAFKA WITH KRaft                           │
  │                                                              │
  │  ┌──────────────────────────────────────────┐               │
  │  │           KRaft QUORUM                   │               │
  │  │                                          │               │
  │  │  ┌────────────┐  ┌────────┐  ┌────────┐  │               │
  │  │  │ Controller │  │ Voter  │  │ Voter  │  │               │
  │  │  │  (Active)  │  │  2     │  │  3     │  │               │
  │  │  └────────────┘  └────────┘  └────────┘  │               │
  │  │                                          │               │
  │  │  Uses RAFT consensus algorithm           │               │
  │  │  Metadata stored in __cluster_metadata   │               │
  │  └──────────────────────────────────────────┘               │
  │                        │                                    │
  │              ┌─────────┼──────────┐                         │
  │              ▼         ▼          ▼                         │
  │          Broker 1  Broker 2   Broker 3                      │
  │          (also     (pure      (pure                         │
  │          voter)    broker)    broker)                       │
  └──────────────────────────────────────────────────────────────┘
```

---

## Comparison

| Feature | Zookeeper Mode | KRaft Mode |
|---------|---------------|-----------|
| External dependency | ✅ Yes (ZK cluster) | ❌ No |
| Max partitions | ~200K | Millions |
| Controller failover | Seconds-minutes | Milliseconds |
| Operational complexity | High | Low |
| Available since | Kafka 0.x | Kafka 2.8 (KIP-500) |
| Production ready | Legacy | Kafka 3.3+ |
| Zookeeper deprecated | — | Kafka 3.5 |

---

## 🚀 Migration Path

```
  Kafka 2.8    → KRaft available (experimental)
  Kafka 3.0    → KRaft in preview
  Kafka 3.3    → KRaft production-ready ✅
  Kafka 3.5    → ZooKeeper deprecated ⚠️
  Kafka 4.0    → ZooKeeper REMOVED ❌
```
