# 🟢 Beginner Level Questions

---

## Q1. What is Apache Kafka?
**A:** Apache Kafka is an open-source distributed event streaming platform. It is used to build real-time data pipelines and streaming applications. It was originally developed at LinkedIn and donated to Apache.

Key properties:
- **High throughput** — millions of messages/second
- **Durable** — messages stored on disk with replication
- **Fault-tolerant** — survives broker failures
- **Scalable** — horizontal scaling via partitions

---

## Q2. What is a Topic in Kafka?
**A:** A topic is a logical channel/category to which producers publish messages and from which consumers read. Think of it as a database table name. Topics are:
- **Append-only** — records are never modified
- **Durable** — retention is configurable (default 7 days)
- **Multi-subscriber** — multiple consumer groups can read simultaneously

---

## Q3. What is a Partition?
A partition is an ordered log where Kafka stores messages. We use multiple partitions mainly for parallel processing and scalability. Replication provides fault tolerance. If a topic has 3 partitions and the consumer group has 5 consumers, at most 3 consumers can actively consume those partitions, and the remaining 2 will stay idle. One consumer can also be assigned multiple partitions.
---

## Q4. What is an Offset?
**A:** An offset is a unique sequential number assigned to each record within a partition. Consumers use offsets to track their position. Kafka does NOT delete records when consumed — the consumer manages its own offset.

---

## Q5. What is a Consumer Group?
**A:** A consumer group is a logical grouping of consumers that work together to consume from a topic. Each partition is consumed by exactly ONE consumer in a group. Multiple groups can consume the same topic independently.

```
Topic: 3 partitions
Group A: [C1→P0, C2→P1, C3→P2]    ← parallel, each reads 1 partition
Group B: [C4→P0,P1,P2]             ← 1 consumer reads all (no parallelism)
```

---

## Q6. What is the difference between Kafka and RabbitMQ?

| Kafka | RabbitMQ |
|-------|---------|
| Event streaming / log | Message queue / broker |
| Pull-based consumers | Push-based consumers |
| Message replay possible | Messages deleted after consume |
| Millions/sec throughput | ~50K/sec |
| Offset-based tracking | ACK-based tracking |
| Best for: streaming, CDC | Best for: task queues, RPC |

---

## Q7. What is a Broker?
**A:** A broker is a single Kafka server. It stores data, handles producer requests, and serves consumer requests. A Kafka cluster consists of multiple brokers for fault tolerance and scalability.

---

## Q8. What is Zookeeper's role in Kafka?
**A:** Zookeeper (being replaced by KRaft) manages:
- Broker registration and discovery
- Controller election (which broker is the controller)
- Topic and partition metadata
- ACLs and configuration
- Consumer group offsets (old API only)

> Note: Kafka 3.3+ supports KRaft mode, removing the Zookeeper dependency entirely.

---

## Q9. What does `auto.offset.reset` do?
**A:** Controls where a consumer starts reading when there is **no committed offset**:
- `earliest` → start from the very beginning of the topic
- `latest` → start from new messages only (ignore old)
- `none` → throw exception if no offset found

---

## Q10. Can Kafka guarantee message ordering?
**A:** Yes, but **only within a partition**. If you need all messages for a given entity (e.g., same user, same order) to be ordered, use that entity's ID as the **message key**. Kafka routes same-key messages to the same partition always.

---

## Q11. What is `acks` in Kafka producer?
**A:**
- `acks=0` → No acknowledgment (fastest, may lose data)
- `acks=1` → Leader acks after writing (default, may lose on leader crash)
- `acks=all` or `acks=-1` → All ISR brokers ack (slowest, no data loss)

---

## Q12. What is a Dead Letter Topic (DLT)?
**A:** A DLT is a special topic where messages are sent when they cannot be processed after all retries. It allows:
- Identifying problematic messages
- Manual investigation and replay
- Preventing one bad message from blocking the entire consumer
