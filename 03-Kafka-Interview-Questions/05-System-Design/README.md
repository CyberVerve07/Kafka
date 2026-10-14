# 🏗️ Kafka System Design Questions

---

## Design 1: Real-Time E-Commerce Order Processing Pipeline

```
[Mobile App / Web]
        │ (HTTP POST)
        ▼
 [Order Service API]
        │
   (Kafka Producer)
        │
        ▼
   Topic: `orders` (Partitions: 12, Key: userId)
        ├──▶ [Inventory Service] (Consumer Group: inventory)
        ├──▶ [Payment Service]   (Consumer Group: payment)
        ├──▶ [Notification Svc] (Consumer Group: notification)
        └──▶ [Analytics Svc]    (Kafka Streams / ClickHouse)
```

### Key Design Considerations:
- **Partition Key**: `userId` or `orderId`
- **Guarantees**: `acks=all`, `min.insync.replicas=2` for financial transactions
- **Idempotency**: Consumers check `orderId` in Redis/DB before processing to prevent duplicate charging.
