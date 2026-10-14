# 🟡 Intermediate Level Questions

---

## Q1. What is the ISR (In-Sync Replicas)?
**A:** ISR is the set of replicas that are fully caught up with the leader partition. A replica is in-sync if it has fetched all messages from the leader within `replica.lag.time.max.ms` (default: 30 seconds).

```
Leader: [0][1][2][3][4]
ISR Replica: [0][1][2][3][4]  ← in sync ✅
Lagging Replica: [0][1]        ← OUT of ISR ❌
```

Importance: `acks=all` waits for ALL ISR replicas to confirm — ensuring no data loss even if leader crashes.

---

## Q2. What happens during a Kafka consumer rebalance?
**A:** A rebalance occurs when the consumer group membership changes (new consumer joins, one crashes, or subscription changes). During rebalance:
1. All consumers **stop consuming** (stop-the-world with old strategies)
2. Group coordinator assigns partitions
3. Consumers restart from their committed offsets

To minimize impact use `CooperativeStickyAssignor` which does incremental rebalancing without stopping all consumers.

---

## Q3. Explain `enable.auto.commit` — when to use true vs false?

| | `true` | `false` |
|-|--------|---------|
| Commit timing | Every `auto.commit.interval.ms` | Manually after processing |
| Risk | May commit before processing done | Developer controls |
| Use case | Simple logs, idempotent ops | Payment, critical data |
| Data loss risk? | Yes (committed but not processed) | No |
| Duplicate risk? | Yes (processed but not committed) | Controlled |

**Best practice**: Use `enable.auto.commit=false` + `MANUAL_IMMEDIATE` ack mode in production.

---

## Q4. What is Log Compaction?
**A:** Log compaction retains only the **latest record per key**, instead of deleting old records by time. Useful for:
- Keeping current state (e.g., user profile, product price)
- Change Data Capture (CDC) snapshots
- Rebuilding state after crash

```
Before: [u1:v1][u2:v1][u1:v2][u2:v2][u1:v3]
After compaction: [u2:v2][u1:v3]   ← only latest per key
```

Enable with: `cleanup.policy=compact`

---

## Q5. What is `min.insync.replicas`?
**A:** The minimum number of ISR replicas that must acknowledge a write when `acks=all`. If fewer replicas are in sync, the producer gets a `NotEnoughReplicasException`.

```
replication.factor = 3
min.insync.replicas = 2
acks = all

→ Data is safe even if 1 broker dies
→ If 2 brokers die → producer blocked (prevents data loss)
```

---

## Q6. Explain Kafka's delivery semantics.

| Semantic | Guarantee | How |
|---------|-----------|-----|
| **At-most-once** | May lose messages, no duplicates | `acks=0`, no retry |
| **At-least-once** | No loss, may duplicate | `acks=all`, retries, idempotent consumer |
| **Exactly-once** | No loss, no duplicates | Idempotent producer + transactions |

---

## Q7. What is `linger.ms` and `batch.size`?
**A:** These control producer batching:
- `batch.size`: Max size of a batch (default 16KB). Producer sends when batch is full.
- `linger.ms`: Time to wait before sending (even if batch not full). Default 0ms.

Higher values = better throughput, higher latency. For real-time: `linger.ms=0`. For bulk: `linger.ms=20`.

---

## Q8. How does Kafka handle backpressure?
**A:** Kafka handles backpressure through:
1. **Producer side**: `buffer.memory` — if buffer full, `send()` blocks for `max.block.ms`
2. **Consumer side**: Pull model — consumer controls its own pace via `max.poll.records`
3. **Partition lag monitoring** — ops team increases consumers or partitions

---

## Q9. What is a Kafka Schema Registry?
**A:** A central service (typically Confluent Schema Registry) that stores and validates Avro/Protobuf/JSON schemas. It ensures:
- Producers and consumers agree on message format
- Schema evolution with backward/forward compatibility
- Prevents "schema drift" breaking consumers

---

## Q10. What is `max.poll.interval.ms`?
**A:** The maximum time between two `poll()` calls. If a consumer takes longer than this to process records, the broker considers it dead and triggers a rebalance.

Fix slow processing by:
- Reducing `max.poll.records` (process fewer records per poll)
- Increasing `max.poll.interval.ms`
- Moving processing to async thread pools
