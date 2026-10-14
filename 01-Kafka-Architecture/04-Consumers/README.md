# 📥 Kafka Consumers — Deep Dive

## What is a Consumer?

A **consumer** is any application that reads (subscribes to) records from Kafka topics. Consumers use a **pull model** — they request data from brokers at their own pace.

---

## 👥 Consumer Groups

The most powerful concept in Kafka consumption:

```
  Topic: "orders" with 3 partitions

  ┌─────────────────────────────────────────────────────────────┐
  │  Consumer Group: "inventory-service" (3 consumers)          │
  │                                                             │
  │  Partition 0 ──────────────▶ Consumer-1                     │
  │  Partition 1 ──────────────▶ Consumer-2                     │
  │  Partition 2 ──────────────▶ Consumer-3                     │
  │                                                             │
  │  ✅ Each partition read by exactly ONE consumer in group     │
  └─────────────────────────────────────────────────────────────┘

  ┌─────────────────────────────────────────────────────────────┐
  │  Consumer Group: "analytics-service" (1 consumer)           │
  │                                                             │
  │  Partition 0 ──────┐                                        │
  │  Partition 1 ──────┼──▶ Consumer-A (reads all 3)            │
  │  Partition 2 ──────┘                                        │
  │                                                             │
  │  ✅ Groups are INDEPENDENT — separate offset tracking       │
  └─────────────────────────────────────────────────────────────┘
```

**Rules:**
- `consumers ≤ partitions` → optimal parallelism
- `consumers > partitions` → extra consumers idle
- Different groups = fully independent reads (each sees all messages)

---

## 🔄 Consumer Group Rebalancing

What happens when a consumer joins or leaves:

```
  BEFORE (3 consumers, 3 partitions):
  Consumer-1 → P0
  Consumer-2 → P1
  Consumer-3 → P2

  Consumer-3 DIES
  ↓
  GROUP COORDINATOR detects heartbeat timeout
  ↓
  REBALANCE triggered
  ↓
  AFTER (2 consumers):
  Consumer-1 → P0, P2  (takes over P2)
  Consumer-2 → P1

  NEW CONSUMER joins
  ↓
  REBALANCE again
  ↓
  Consumer-1 → P0
  Consumer-2 → P1
  Consumer-New → P2
```

---

## 📍 Offset Management

```
  Consumer commits its offset to track progress:
  
  Topic: "orders", Partition 0
  ┌───┬───┬───┬───┬───┬───┬───┐
  │ 0 │ 1 │ 2 │ 3 │ 4 │ 5 │ 6 │
  └───┴───┴───┴───┴───┴───┴───┘
              ↑
         committed offset=2
         (consumer will resume from 3 after restart)

  Offsets stored in: __consumer_offsets (internal Kafka topic)
```

### Offset Reset Strategies

| Strategy | Behavior |
|----------|---------|
| `earliest` | Start from the beginning of the topic |
| `latest` | Start from new messages only |
| `none` | Throw error if no committed offset |

---

## ⚙️ Key Consumer Configurations

| Config | Default | Description |
|--------|---------|-------------|
| `group.id` | (required) | Consumer group name |
| `auto.offset.reset` | `latest` | Where to start if no offset |
| `enable.auto.commit` | `true` | Auto-commit offsets periodically |
| `auto.commit.interval.ms` | `5000` | How often to auto-commit |
| `max.poll.records` | `500` | Max records per poll |
| `session.timeout.ms` | `45000` | Heartbeat timeout |
| `heartbeat.interval.ms` | `3000` | Heartbeat frequency |
| `fetch.min.bytes` | `1` | Min data to fetch |
| `fetch.max.wait.ms` | `500` | Max wait if < fetch.min.bytes |

---

## 🔀 Partition Assignment Strategies

| Strategy | Description | Use Case |
|----------|------------|---------|
| `RangeAssignor` | Assigns contiguous ranges | Default, simple |
| `RoundRobinAssignor` | Even distribution | Balanced load |
| `StickyAssignor` | Minimizes partition movement on rebalance | Production preferred |
| `CooperativeStickyAssignor` | Incremental rebalancing (no stop-the-world) | Best for large clusters |
