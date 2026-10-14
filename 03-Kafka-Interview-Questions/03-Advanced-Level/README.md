# 🔴 Advanced Level Questions

---

## Q1. How does Kafka achieve Exactly-Once Semantics (EOS)?
**A:** Kafka achieves EOS via two main mechanisms:
1. **Idempotent Producer (`enable.idempotence=true`)**:
   - Assigns a unique Producer ID (`PID`) and monotonic sequence numbers to messages per partition.
   - Broker deduplicates incoming messages if sequence number is already committed.
2. **Transactional API (`transactional.id`)**:
   - Coordinates across multiple partitions and topics using a Transaction Coordinator and a two-phase commit protocol.
   - `read_committed` isolation level on consumers ensures only committed transactions are read.

---

## Q2. What causes "Consumer Lag" and how to debug it?
**A:** Consumer lag is the difference between the latest offset written to a partition and the offset committed by the consumer group.
- **Causes**:
  - Slow business logic / database locks in consumer
  - CPU/Memory bottleneck on consumer instance
  - Partitions skewed / uneven key distribution
  - Consumer crashes or frequent rebalancing
- **Fixes**:
  - Increase consumer instances (up to number of partitions)
  - Tune `max.poll.records` and optimize processing
  - Asynchronous / batch processing inside consumer

---

## Q3. Explain Zero-Copy in Kafka. Why is Kafka so fast?
**A:** Kafka bypasses user-space buffer copies using Linux kernel's `sendfile()` system call (Transfer-To in Java NIO):
```
Standard Copy:
Disk -> OS Cache -> User Buffer -> Socket Buffer -> NIC Buffer (4 copies, 4 context switches)

Kafka Zero-Copy:
Disk -> OS Page Cache -------------------------> NIC Buffer (Zero CPU copies, direct transfer)
```
Combined with sequential disk I/O, batching, and OS page cache, Kafka delivers near-network-limit throughput.
