# 📤 Kafka Producers — Deep Dive

## What is a Producer?

A **producer** is any application that publishes (writes) records to Kafka topics.

---

## 🏗️ Producer Internal Architecture

```
  ┌──────────────────────────────────────────────────────────────────────────┐
  │                        KAFKA PRODUCER INTERNALS                         │
  │                                                                          │
  │  Your Application                                                        │
  │       │                                                                  │
  │       ▼  producer.send(record)                                           │
  │  ┌────────────────────────────────────────────────────────────────────┐  │
  │  │  1. SERIALIZER                                                     │  │
  │  │     Key: StringSerializer                                          │  │
  │  │     Value: JsonSerializer / AvroSerializer                        │  │
  │  └────────────────────────────┬───────────────────────────────────────┘  │
  │                               │                                          │
  │  ┌────────────────────────────▼───────────────────────────────────────┐  │
  │  │  2. PARTITIONER                                                    │  │
  │  │     • If key != null → hash(key) % numPartitions                  │  │
  │  │     • If key == null → Round Robin                                │  │
  │  │     • Custom Partitioner → your logic                             │  │
  │  └────────────────────────────┬───────────────────────────────────────┘  │
  │                               │                                          │
  │  ┌────────────────────────────▼───────────────────────────────────────┐  │
  │  │  3. RECORD ACCUMULATOR (Buffer)                                    │  │
  │  │     Partition 0 Queue: [rec1, rec2, rec3] ← batch.size = 16KB    │  │
  │  │     Partition 1 Queue: [rec1]                                     │  │
  │  │     Partition 2 Queue: [rec1, rec2]                               │  │
  │  │                                                                   │  │
  │  │     Triggers flush when:                                          │  │
  │  │     • batch.size reached (16384 bytes default)                    │  │
  │  │     • linger.ms elapsed (0ms default)                             │  │
  │  └────────────────────────────┬───────────────────────────────────────┘  │
  │                               │                                          │
  │  ┌────────────────────────────▼───────────────────────────────────────┐  │
  │  │  4. SENDER THREAD                                                  │  │
  │  │     Picks batches and sends to correct broker                     │  │
  │  │     Handles retries on failure                                    │  │
  │  └────────────────────────────┬───────────────────────────────────────┘  │
  └───────────────────────────────┼──────────────────────────────────────────┘
                                  │
                       ┌──────────▼──────────┐
                       │    KAFKA BROKER     │
                       └─────────────────────┘
```

---

## ⚙️ Critical Producer Configurations

### `acks` — Acknowledgment Level

```
  acks=0  (Fire and Forget)
  Producer ──────────────────────────▶ Broker
           ←── no wait ───

  acks=1  (Leader Ack)
  Producer ──────────────────────────▶ Leader Broker
           ◀──── ack when written ───
           (data could be lost if leader crashes before replication)

  acks=all / acks=-1  (Strongest guarantee)
  Producer ──────────────────────────▶ Leader Broker
                                             │
                                    ─────────┼────────
                                    │replicates to ISR│
                                    ─────────┼────────
           ◀──── ack when ALL ISR written ───
```

| `acks` | Throughput | Durability | Use Case |
|--------|-----------|-----------|---------|
| `0` | Highest | None | Metrics, logs |
| `1` | High | Medium | General |
| `all` | Lower | Strongest | Payments, orders |

---

## 🔄 Producer Retry Flow

```
  send(record)
       │
  Network Error? ──── YES ──── retries > 0? ──── YES ──── wait retry.backoff.ms
       │                                                         │
       NO                                          ◀────────── retry
       │
  Delivery.timeout.ms exceeded? ──── YES ──── throw Exception
       │
       NO
       │
  ◀── Success ──
```

---

## 📋 Key Producer Configs Summary

| Config | Default | Description |
|--------|---------|-------------|
| `acks` | `1` | Acknowledgment level |
| `retries` | `2147483647` | Number of retries |
| `batch.size` | `16384` | Max batch size in bytes |
| `linger.ms` | `0` | Wait time before sending batch |
| `buffer.memory` | `33554432` | Total buffer memory |
| `compression.type` | `none` | `gzip`, `snappy`, `lz4`, `zstd` |
| `max.in.flight.requests` | `5` | Max unacked requests per connection |
| `enable.idempotence` | `true` | Prevent duplicate sends |
