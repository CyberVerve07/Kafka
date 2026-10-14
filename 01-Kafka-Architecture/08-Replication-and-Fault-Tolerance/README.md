# 🔁 Replication & Fault Tolerance

---

## Replication Architecture

```
  Topic: "orders", Partition 0, Replication Factor = 3

  ┌──────────────────────────────────────────────────────────────────┐
  │  Broker 1 ★ LEADER (P0)                                         │
  │  Log: [msg0][msg1][msg2][msg3][msg4]                             │
  │                 │                                                │
  │         Replicates to ISR                                        │
  │         ┌───────┴───────┐                                        │
  │         ▼               ▼                                        │
  │  Broker 2 FOLLOWER    Broker 3 FOLLOWER                         │
  │  Log: [msg0][msg1]    Log: [msg0][msg1][msg2]                   │
  │  (slightly behind)    (almost caught up)                         │
  │                                                                  │
  │  ISR (In-Sync Replicas) = {Broker1, Broker3}                    │
  │  Broker2 is out of ISR (too far behind)                         │
  └──────────────────────────────────────────────────────────────────┘
```

---

## ISR — In-Sync Replicas

```
  ISR = set of replicas caught up with the leader

  A replica is IN-SYNC if:
  • It has fetched from leader within replica.lag.time.max.ms (30s default)
  • It is not more than replica.lag.max.messages behind

  Leader failure scenario:
  ISR = {Broker1★, Broker2, Broker3}
  
  Broker1 crashes!
  ↓
  Controller elects new leader from ISR
  → Broker2 or Broker3 becomes new leader
  → NO data loss (both were in-sync)

  If ISR = {Broker1★} only (others lagging):
  → unclean.leader.election.enable=true → elect out-of-ISR (data loss risk!)
  → unclean.leader.election.enable=false → topic goes offline (safer)
```

---

## ⚙️ Replication Configuration

| Config | Recommended | Description |
|--------|------------|-------------|
| `replication.factor` | 3 | Number of copies |
| `min.insync.replicas` | 2 | Min replicas for acks=all |
| `unclean.leader.election.enable` | `false` | Prevent data loss |
| `replica.lag.time.max.ms` | 30000 | Max lag before removing from ISR |

---

## 🛡️ The Golden Rule

```
  For zero data loss in production:

  Producer:    acks = all
  Topic:       replication.factor = 3
  Broker:      min.insync.replicas = 2

  This means: data is written to at least 2 brokers before ack
  Even if 1 broker dies → no data loss ✅
```
