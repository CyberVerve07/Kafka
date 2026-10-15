# 🌋 Extreme Hard-Level Kafka Scenarios & Production Post-Mortems

> In-depth analysis of 14 extreme, production-grade Kafka scenarios asked in Staff / Principal Engineer and Architect-level system design and debugging rounds at top tech companies.

---

## 📑 Table of Contents

1. [Payment Processing Consistency: Dual-Write Database & Kafka](#-scenario-1-payment-processing-consistency-dual-write-problem)
2. [Consumer Crash Handling: 200 Orders Lost Silently](#-scenario-2-consumer-crash-handling-silent-message-loss)
3. [Backlog Clearance Spike: Adding Consumers Fails to Clear 5M Lag](#-scenario-3-backlog-clearance-during-traffic-spikes)
4. [Hot Partition Skew: Celebrity Traffic Overloading Single Partition](#-scenario-4-hot-partition-issue-celebrity-traffic-skew)
5. [Re-Processing Bad Data: Replaying 2 Hours Without Duplication](#-scenario-5-re-processing-bad-data-without-creating-duplicates)
6. [Out-of-Order Delivery: Delivered Arrives Before Order Placed](#-scenario-6-order-of-operations-out-of-order-events)
7. [Log Compaction: Keeping Latest User Profiles Only](#-scenario-7-compacted-topics-latest-state-preservation)
8. [Producer Throughput Scaling: 200 msg/sec to 50,000 msg/sec](#-scenario-8-producer-throughput-scaling)
9. [Cascading Failure & Outages: Downstream Service Down with Retries](#-scenario-9-handling-downstream-service-outages)
10. [High-Scale Telemetry: 1 Million Drivers Live Location Tracking](#-scenario-10-live-location-tracking-system-at-scale)
11. [Data Retention Mystery: 3-Day-Old Data Reported Missing](#-scenario-11-data-retention-and-recovery-investigation)
12. [Dual-State Failure: Database Saved but Kafka Offset Commit Fails](#-scenario-12-consumer-save-failure-db-ok-offset-commit-fails)
13. [Broker Disk Exhaustion: Sudden Overnight Disk Full](#-scenario-13-broker-disk-space-sudden-exhaustion)
14. [Consumer Performance Blast Radius: Impact of 1 Slow Service in Group](#-scenario-14-consumer-performance-impact-in-a-group)

---

## 💳 Scenario 1: Payment Processing Consistency (Dual-Write Problem)

> **Interviewer asks:** "In our payment microservice, an incoming charge must be saved into our SQL database AND published as a `PaymentCompleted` event to Kafka. If the database commit succeeds but the Kafka publish fails (network timeout, broker down), or vice versa, our state becomes inconsistent. How do you guarantee that every payment saved to the database is reliably sent to Kafka without dual-write inconsistency?"

### 1. Root Cause: The Dual-Write Hazard
When an application attempts to write to two independent distributed systems (e.g., PostgreSQL and Apache Kafka) within the same API request, distributed consistency cannot be guaranteed with naive consecutive writes:

```
[Client Request]
       │
       ▼
 1. Write to DB ───► [SUCCESS]
       │
 2. Send to Kafka ─► [BROKER TIMEOUT / JVM CRASH / NETWORK DROP]
       │
       ▼
 [INCONSISTENCY]: DB has the payment, but downstream services 
                  (Ledger, Notifications, Fulfillment) NEVER receive it!
```

- Reversing the order (publish to Kafka first, then commit to DB) causes the reverse failure: an event is published, but the DB transaction rolls back, producing **phantom events**.
- Distributed Two-Phase Commit (2PC / XA) is slow, fragile, and not natively supported across heterogeneous systems like Kafka and relational databases.

---

### 2. Industry-Standard Architecture Solutions

#### Option A: Transactional Outbox Pattern with Change Data Capture (CDC) — *Recommended for Production*
Instead of directly publishing to Kafka from application code, the payment transaction and an outbox record are committed atomically inside the **same local database transaction**.

```
┌────────────────────────────────────────────────────────────────────────┐
│                        PAYMENT MICROSERVICE                            │
│                                                                        │
│   @Transactional                                                       │
│   1. INSERT INTO payments (id, amount, status)                         │
│   2. INSERT INTO outbox (id, aggregate_type, payload, created_at)      │
│   3. COMMIT DB TRANSACTION (Atomic Local ACID Guarantee)               │
└──────────────────────────────────┬─────────────────────────────────────┘
                                   │
                                   ▼
                      ┌────────────────────────┐
                      │    POSTGRESQL WAL /    │
                      │       MYSQL BINLOG     │
                      └────────────┬───────────┘
                                   │
                                   ▼
                      ┌────────────────────────┐
                      │  DEBEZIUM CDC CONNECTOR│
                      │  (Kafka Connect Engine)│
                      └────────────┬───────────┘
                                   │
                                   ▼
                      ┌────────────────────────┐
                      │  KAFKA TOPIC:          │
                      │  `payment-events`      │
                      └────────────────────────┘
```

#### Step-by-Step Implementation:
1. **Database Schema:**
   ```sql
   CREATE TABLE payments (
       id VARCHAR(64) PRIMARY KEY,
       account_id VARCHAR(64) NOT NULL,
       amount DECIMAL(12, 2) NOT NULL,
       status VARCHAR(20) NOT NULL,
       created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
   );

   CREATE TABLE outbox_events (
       id UUID PRIMARY KEY,
       aggregate_type VARCHAR(50) NOT NULL,
       aggregate_id VARCHAR(64) NOT NULL,
       event_type VARCHAR(50) NOT NULL,
       payload JSONB NOT NULL,
       created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
   );
   ```

2. **Spring Boot Service Code:**
   ```java
   @Service
   @RequiredArgsConstructor
   public class PaymentService {

       private final PaymentRepository paymentRepo;
       private final OutboxRepository outboxRepo;
       private final ObjectMapper objectMapper;

       @Transactional
       public Payment processPayment(PaymentRequest req) {
           // 1. Save payment entity
           Payment payment = new Payment(req.getPaymentId(), req.getAccountId(), req.getAmount(), "COMPLETED");
           paymentRepo.save(payment);

           // 2. Save event in Outbox table within the exact same DB transaction
           PaymentEvent event = new PaymentEvent(payment.getId(), payment.getAmount(), "PAYMENT_COMPLETED");
           OutboxEvent outbox = OutboxEvent.builder()
                   .id(UUID.randomUUID())
                   .aggregateType("PAYMENT")
                   .aggregateId(payment.getId())
                   .eventType("PAYMENT_COMPLETED")
                   .payload(objectMapper.valueToTree(event))
                   .createdAt(Instant.now())
                   .build();
           outboxRepo.save(outbox);

           // 3. Method returns -> Database transaction commits atomically
           return payment;
       }
   }
   ```

3. **Kafka Connect Debezium Configuration:**
   ```json
   {
     "name": "payment-outbox-connector",
     "config": {
       "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
       "tasks.max": "1",
       "plugin.name": "pgoutput",
       "database.hostname": "postgres-db",
       "database.port": "5432",
       "database.user": "debezium",
       "database.password": "secret",
       "database.dbname": "orders_db",
       "database.server.name": "pg-payment",
       "table.include.list": "public.outbox_events",
       "tombstones.on.delete": "false",
       "transforms": "outbox",
       "transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",
       "transforms.outbox.route.topic.replacement": "payment-events"
     }
   }
   ```

---

#### Option B: Transactional Poller Pattern (When CDC is not possible)
If infrastructure cannot host Kafka Connect:
1. Save payment + outbox record with `status = 'PENDING'` in a single DB transaction.
2. A scheduled background worker (with advisory locks like `pg_try_advisory_xact_lock` or ShedLock) polls pending records using `SELECT ... FOR UPDATE SKIP LOCKED LIMIT 100`.
3. Worker publishes batch to Kafka with `acks=all`.
4. Upon successful Kafka acknowledgment, worker marks records `status = 'PROCESSED'`.

---

### 3. Interview Pro-Tips & Edge Cases
- **Why not Kafka Transactions alone?** A Kafka transaction coordinates *multiple Kafka partitions and topics*, but it cannot roll back or commit an external SQL database without an XA transaction manager.
- **Deduplication requirement:** CDC guarantees **at-least-once** delivery to Kafka. Downstream consumers must implement idempotency based on `event.id` or `payment.id`.

---

## 🛑 Scenario 2: Consumer Crash Handling (Silent Message Loss)

> **Interviewer asks:** "A consumer instance crashes unexpectedly, and 200 orders are silently lost without any exceptions or errors in the logs. When the replacement consumer starts up, it continues from a later offset. What caused this message loss, and how do you guarantee zero data loss?"

### 1. Root Cause Analysis
This classic defect occurs when offsets are committed **before** messages are fully processed and persisted. Two primary culprits cause this:

#### Culprit 1: Automatic Offset Commit (`enable.auto.commit = true`)
By default, Kafka consumer configuration has:
- `enable.auto.commit = true`
- `auto.commit.interval.ms = 5000` (5 seconds)

```
Time 0s: consumer.poll() receives 200 records (offsets 100 to 299).
Time 3s: While records are still being processed in a loop, the background 
         heartbeat thread fires auto-commit, committing offset 300 to Kafka!
Time 4s: Consumer crashes (OutOfMemory, SIGKILL, pod eviction).
Result:  Replacement consumer boots up, reads committed offset 300.
         Offsets 100-299 are SKIPPED forever!
```

#### Culprit 2: Asynchronous Worker Thread Pool with Synchronous Polling
```java
// DANGEROUS ANTI-PATTERN:
ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
for (ConsumerRecord<String, String> record : records) {
    workerExecutor.submit(() -> processOrder(record)); // Fire and forget!
}
consumer.commitSync(); // COMMITS IMMEDIATELY while worker threads are still running!
```
If the container dies while tasks are queued in `workerExecutor`, all in-flight orders vanish.

---

### 2. Comprehensive Production Fix

```
┌─────────────────────────────────────────────────────────────────────────┐
│                 SAFE MANUAL OFFSET COMMIT LIFECYCLE                    │
│                                                                         │
│   consumer.poll()                                                       │
│          │                                                              │
│          ▼                                                              │
│   Iterate Records:                                                      │
│   ├── Parse Order                                                       │
│   ├── Execute Business Logic (Validate, Update DB)                     │
│   └── Confirm DB Transaction Committed                                  │
│          │                                                              │
│          ▼                                                              │
│   MANUAL COMMIT: consumer.commitSync(offsets) / AckMode.MANUAL_IMMEDIATE│
│          │                                                              │
│          ▼                                                              │
│   Next consumer.poll()                                                  │
└─────────────────────────────────────────────────────────────────────────┘
```

#### Spring Boot Production Configuration (`application.yml`):
```yaml
spring:
  kafka:
    consumer:
      enable-auto-commit: false
      auto-offset-reset: earliest
      max-poll-records: 50
    listener:
      ack-mode: manual_immediate # Commit offset immediately after record processing
```

#### Spring Kafka Listener Implementation:
```java
@Component
@Slf4j
public class SafeOrderConsumer {

    @Autowired
    private OrderProcessingService orderProcessingService;

    @KafkaListener(topics = "orders-topic", groupId = "order-fulfillment-group")
    public void listen(ConsumerRecord<String, OrderDto> record, Acknowledgment ack) {
        try {
            log.info("Processing orderId: {}, partition: {}, offset: {}",
                     record.key(), record.partition(), record.offset());
            
            // 1. Synchronously process & persist into Database
            orderProcessingService.processAndPersistOrder(record.value());
            
            // 2. Commit offset ONLY after DB transaction has successfully committed
            ack.acknowledge();
            
        } catch (TransientDatabaseException ex) {
            log.error("Transient error processing offset {}. Throwing for retry.", record.offset(), ex);
            // Do NOT acknowledge. Spring Kafka ErrorHandler will re-deliver or pause
            throw ex;
        }
    }
}
```

#### Native Kafka Consumer Implementation:
```java
while (running) {
    ConsumerRecords<String, Order> records = consumer.poll(Duration.ofMillis(1000));
    for (ConsumerRecord<String, Order> record : records) {
        try {
            processAndSaveToDatabase(record.value());
            
            // Commit specific partition offset synchronously
            TopicPartition tp = new TopicPartition(record.topic(), record.partition());
            consumer.commitSync(Collections.singletonMap(
                tp, new OffsetAndMetadata(record.offset() + 1)
            ));
        } catch (Exception e) {
            logger.error("Failed at offset: {}", record.offset(), e);
            // Seek back to failed offset or route to Dead Letter Queue
            consumer.seek(new TopicPartition(record.topic(), record.partition()), record.offset());
            break;
        }
    }
}
```

---

### 3. Graceful Shutdown & Rebalance Safety
When pods are terminated by Kubernetes (SIGTERM):
1. Register JVM shutdown hook:
   ```java
   Runtime.getRuntime().addShutdownHook(new Thread(() -> {
       logger.info("Graceful shutdown initiated...");
       consumer.wakeup(); // Causes poll() to throw WakeupException safely
   }));
   ```
2. Implement `ConsumerRebalanceListener`: Commit processed offsets during partition revocation before partitions are handed over to another consumer.

---

## 📈 Scenario 3: Backlog Clearance During Traffic Spikes

> **Interviewer asks:** "During a flash sale, consumer lag on an `orders` topic spikes to 5 million messages. The DevOps team immediately scales the consumer deployment from 8 instances to 30 instances in the same consumer group. However, lag clearance throughput does not increase at all. Why did adding consumers fail, and how do you actually clear this backlog?"

### 1. Root Cause Analysis
There are three fundamental architectural reasons why scaling consumer pods failed:

```
TOPIC: orders (Partitions: 8)
┌───────────┬───────────┬───────────┬───────────┬───────────┬───────────┬───────────┬───────────┐
│Part-0 (Lag│Part-1 (Lag│Part-2 (Lag│Part-3 (Lag│Part-4 (Lag│Part-5 (Lag│Part-6 (Lag│Part-7 (Lag│
│  625k)    │  625k)    │  625k)    │  625k)    │  625k)    │  625k)    │  625k)    │  625k)    │
└─────┬─────┴─────┬─────┴─────┬─────┴─────┬─────┴─────┬─────┴─────┬─────┴─────┬─────┴─────┬─────┘
      │           │           │           │           │           │           │           │
   Consumer1   Consumer2   Consumer3   Consumer4   Consumer5   Consumer6   Consumer7   Consumer8

   Consumers 9 through 30: ❌ COMPLETELY IDLE! (Assigned 0 partitions)
```

1. **The Partition Ceiling Rule:**
   - **Kafka Rule:** A single partition within a topic can only be assigned to **at most one consumer instance** within the same consumer group.
   - If the topic has **8 partitions**, having 30 consumers means 8 consumers are active and **22 consumers are sitting completely idle**, consuming 0 messages.
2. **Key Skew (Hot Partitions):**
   - If the producer published with an uneven key distribution, 4.5 million out of the 5 million messages might be sitting in just 1 partition. Adding consumers cannot help that single overwhelmed partition.
3. **Consumer Rebalance Storms:**
   - Scaling up 22 pods simultaneously triggers multiple eager rebalances, pausing all consumption across the entire group repeatedly ("Stop-the-World" rebalance effect).

---

### 2. Multi-Phase Resolution Plan

#### Phase 1: In-Memory Worker Parallelism (Immediate Fix without Altering Partitions)
When you cannot alter partition count immediately (or altering partitions would break strict key ordering), use **sub-partition parallel processing** inside each active consumer using an in-memory worker thread pool or Confluent's **Parallel Consumer**:

```
                       ┌────────────────────────┐
                       │ Consumer 1 (Part-0)    │
                       └───────────┬────────────┘
                                   │
                    ┌──────────────┼──────────────┐
                    ▼              ▼              ▼
              Worker Thread 1 Worker Thread 2 Worker Thread 3
              (Order 1..100)  (Order 101..200) (Order 201..300)
```

```java
// Using Virtual Threads or bounded ExecutorService with manual offset tracking
@KafkaListener(topics = "orders", groupId = "order-group", concurrency = "8")
public void listenBatch(List<ConsumerRecord<String, Order>> records, Acknowledgment ack) {
    // Process records in parallel using Java 21 Virtual Threads
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
        List<Future<Void>> futures = records.stream()
            .map(record -> executor.submit(() -> {
                processSingleOrder(record.value());
                return null;
            }))
            .toList();

        // Wait for batch completion
        for (var f : futures) {
            f.get(5, TimeUnit.SECONDS);
        }
        ack.acknowledge();
    } catch (Exception e) {
        log.error("Batch processing failed", e);
    }
}
```

#### Phase 2: Alter Topic Partitions (Structural Fix)
Scale the topic partitions dynamically to match the desired consumer count:

```bash
# 1. Scale topic partitions from 8 to 32
kafka-topics.sh --bootstrap-server localhost:9092 \
  --alter --topic orders --partitions 32

# 2. Verify all 32 partitions are active
kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic orders
```

> [!WARNING]
> **Key Ordering Warning:** Increasing partitions changes `hash(key) % num_partitions`. Existing keys will map to different partition numbers going forward! If order between past and future events for the same key is strictly required, use Phase 1 (Parallel Consumer) or drain existing lag before altering partitions.

#### Phase 3: Switch to Cooperative Sticky Rebalance
Prevent stop-the-world pauses when consumer pods scale:
```properties
partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor
```

---

## 🔥 Scenario 4: Hot Partition Issue (Celebrity Traffic Skew)

> **Interviewer asks:** "We run a social network. When a celebrity like Cristiano Ronaldo or Elon Musk posts, 80% of incoming engagement events (likes, views, comments) carry `authorId = 'ronaldo_7'`. As a result, Partition 4 experiences a massive hotspot with CPU at 100% and lagging by millions, while Partitions 0-3 and 5-15 are completely idle. How do you distribute this load evenly?"

### 1. Root Cause Analysis
By default, Kafka producers use `murmur2(key) % num_partitions`.
- When all incoming likes carry the exact same key (`authorId`), every single event hashes to the identical partition.
- A single broker replica handles all the I/O, and a single consumer thread handles all the processing, completely breaking distributed scaling.

```
Incoming Likes:
[ronaldo_7] ──┐
[ronaldo_7] ──┼─── Default Partitioner ───► Partition 4 (80% Load, Lag: 3.5M)
[ronaldo_7] ──┘                             Partition 0 (1% Load)
                                            Partition 1 (2% Load)
                                            Partition 2 (1% Load)
                                            Partition 3 (2% Load)
```

---

### 2. Solutions & Architecture Patterns

#### Solution 1: Key Salting / Suffixing (Best for Ordering per Sub-Bucket)
Append a pseudo-random or modulo salt to hot keys:

```
Original Key: "ronaldo_7"
Salted Keys:  "ronaldo_7#1", "ronaldo_7#2", "ronaldo_7#3", ... "ronaldo_7#10"
```

Each salted key hashes to a distinct partition, scattering the 80% load evenly across 10 partitions!

```java
public class SaltedKeyPartitioner {
    private static final Set<String> KNOWN_CELEBRITY_IDS = Set.of("ronaldo_7", "musk_x", "taylor_13");
    private static final int SALT_FACTOR = 10;
    private final Random random = new Random();

    public String computePartitionKey(String authorId) {
        if (KNOWN_CELEBRITY_IDS.contains(authorId)) {
            int salt = random.nextInt(SALT_FACTOR);
            return authorId + "#" + salt; // Distributes over 10 partitions
        }
        return authorId; // Standard key for normal users
    }
}
```

#### Downstream Aggregator (Scatter-Gather):
When querying or aggregating total likes for `ronaldo_7`:
- A Kafka Streams state store or Redis counter aggregates counts across all `ronaldo_7#*` buckets using `INCRBY` or stream tumbling windows:
  $$\text{Total Likes} = \sum_{i=0}^{9} \text{Count}(\text{"ronaldo\_7\#" + } i)$$

---

#### Solution 2: Custom Partitioner with Dynamic Hot-Key Detection
If celebrity IDs are not statically known in advance, implement a dynamic local frequency counter (e.g., using Count-Min Sketch or Guava cache) inside the producer:
- If key frequency in the last 10 seconds exceeds a threshold (e.g., 500 events/sec), automatically apply salting or round-robin routing.

#### Solution 3: Decouple Partition Key from Author ID
Ask the critical product question: **Do likes on a post require strict FIFO ordering across the globe?**
- In 99% of social media systems, likes do **not** require strict ordering.
- **Fix:** Pass `null` as the Kafka message key, or use `eventId` as key.
- Kafka’s built-in `UniformStickyPartitioner` will distribute batches evenly across all partitions in round-robin fashion, eliminating hot spots entirely.

---

## 🔄 Scenario 5: Re-Processing Bad Data (Without Creating Duplicates)

> **Interviewer asks:** "A bad code deployment at 2:00 PM introduced a calculation bug in our financial reconciliation pipeline. The bug was detected and patched at 4:00 PM. We need to re-process all messages published between 2:00 PM and 4:00 PM. How do you rewind consumer offsets to 2:00 PM and re-process the data without creating duplicate entries or corrupting downstream databases?"

### 1. Step-by-Step Recovery Architecture

```
                  ┌────────────────────────────────────────────────────────┐
                  │                 TIME-BASED OFFSET RESET                │
                  │                                                        │
                  │  Target Time: 2:00 PM (Epoch: 1718028000000)          │
                  │  kafka-consumer-groups --reset-offsets --to-datetime   │
                  └──────────────────────────┬─────────────────────────────┘
                                             │
                                             ▼
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                             IDEMPOTENT CONSUMER WORKFLOW                                │
│                                                                                          │
│   Record Arrives (Offset rewound to 2:00 PM)                                             │
│          │                                                                               │
│          ▼                                                                               │
│   Check Deduplication Store / DB Primary Key                                            │
│   ┌──────────────────────────────────────────────┐                                       │
│   │ SELECT version, status FROM transactions     │                                       │
│   │ WHERE tx_id = :id;                           │                                       │
│   └──────────────────────┬───────────────────────┘                                       │
│                          │                                                               │
│             ┌────────────┴────────────┐                                                  │
│             ▼                         ▼                                                  │
│      [Record Exists]           [Record Absent]                                           │
│   Compare checksum / payload   Standard INSERT                                           │
│   If identical -> SKIP                                                                   │
│   If faulty -> UPDATE / RECALCULATE                                                      │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

---

### 2. Detailed Technical Execution

#### Step 1: Stop the Active Consumer Group
```bash
# Scale consumer pods to 0 in Kubernetes to prevent offset commit race conditions
kubectl scale deployment reconciliation-consumer --replicas=0
```

#### Step 2: Query and Reset Offsets Using the Timestamps API
Reset offsets specifically to the 2:00 PM timestamp:
```bash
kafka-consumer-groups.sh --bootstrap-server kafka-broker:9092 \
  --group reconciliation-group \
  --topic transactions \
  --reset-offsets \
  --to-datetime 2026-06-10T14:00:00.000 \
  --dry-run

# Once verified, execute:
kafka-consumer-groups.sh --bootstrap-server kafka-broker:9092 \
  --group reconciliation-group \
  --topic transactions \
  --reset-offsets \
  --to-datetime 2026-06-10T14:00:00.000 \
  --execute
```

#### Step 3: Implement Idempotent Database Upsert (Prevent Duplicates)
Instead of blind `INSERT` statements, the consumer uses SQL `ON CONFLICT` (PostgreSQL) or `UPSERT`:

```sql
INSERT INTO transaction_records (tx_id, customer_id, amount, recalculated_fee, updated_at)
VALUES (:txId, :customerId, :amount, :recalculatedFee, NOW())
ON CONFLICT (tx_id) DO UPDATE SET
    recalculated_fee = EXCLUDED.recalculated_fee,
    updated_at = NOW()
WHERE transaction_records.recalculated_fee IS DISTINCT FROM EXCLUDED.recalculated_fee;
```

#### Step 4: Programmatic Alternative using Java `offsetsForTimes`
```java
Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
long targetEpochMs = Instant.parse("2026-06-10T14:00:00Z").toEpochMilli();

for (TopicPartition partition : consumer.assignment()) {
    timestampsToSearch.put(partition, targetEpochMs);
}

// Fetch exact offsets corresponding to timestamp
Map<TopicPartition, OffsetAndTimestamp> foundOffsets = consumer.offsetsForTimes(timestampsToSearch);

for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : foundOffsets.entrySet()) {
    if (entry.getValue() != null) {
        consumer.seek(entry.getKey(), entry.getValue().offset());
    }
}
```

---

## 📦 Scenario 6: Order of Operations (Out-of-Order Events)

> **Interviewer asks:** "A customer receives an SMS stating 'Your package has been Delivered' before receiving the 'Order Placed' confirmation SMS. What architecture flaws cause events to arrive out-of-order in a distributed Kafka setup, and how do you guarantee chronological processing?"

### 1. Root Cause Breakdown
Events can be reordered at four distinct architectural layers:

```
  PRODUCER LAYER                  KAFKA BROKER LAYER                 CONSUMER LAYER
┌─────────────────┐             ┌────────────────────┐            ┌─────────────────┐
│ Producer retry  │             │ Multiple Topics or │            │ Multi-threaded  │
│ reorders packets│ ──────────► │ Keyless Partitions │ ─────────► │ Worker Pool     │
│ on network hiccup             │ (Part-0 vs Part-1) │            │ finishes out of │
└─────────────────┘             └────────────────────┘            │ order           │
                                                                  └─────────────────┘
```

1. **Producer Retries without Idempotence (`max.in.flight.requests.per.connection > 1`):**
   - Batch 1 (Order Placed) fails due to transient network glitch.
   - Batch 2 (Order Delivered) succeeds on the wire.
   - Batch 1 retries and succeeds *after* Batch 2.
2. **Missing or Inconsistent Partition Keys:**
   - `OrderPlaced` sent without a key (goes to Partition 2).
   - `OrderDelivered` sent with `orderId` as key (goes to Partition 5).
   - Partition 5 consumer runs faster than Partition 2 consumer.
3. **Multi-Threaded Consumer Dispatch:**
   - Single consumer thread fetches both messages from Partition 2 in order, but hands them to a thread pool where Thread B (Delivered) finishes execution before Thread A (Placed).

---

### 2. Complete End-to-End Prevention Strategy

#### Strategy 1: Producer Idempotence & In-Flight Request Tuning
Ensure strict ordering on the producer side even across network retries:
```properties
enable.idempotence=true
acks=all
max.in.flight.requests.per.connection=5
retries=2147483647
```
> [!NOTE]
> When `enable.idempotence=true`, Kafka allows up to 5 in-flight requests while still preserving strict chronological ordering because the broker enforces monotonic sequence numbers per producer ID.

#### Strategy 2: Single Partition Key per Business Aggregate
Every lifecycle event for an order (`ORDER_CREATED`, `PAYMENT_PROCESSED`, `SHIPPED`, `DELIVERED`) **MUST** use the exact same key: `order_id`.
```java
ProducerRecord<String, OrderEvent> record = 
    new ProducerRecord<>("order-lifecycle-events", order.getOrderId(), orderEvent);
producer.send(record);
```

#### Strategy 3: Consumer State Machine Validation (Defensive Handling)
Consumers must validate lifecycle state transitions before persisting:

```java
@Transactional
public void handleEvent(OrderEvent event) {
    Order order = orderRepo.findById(event.getOrderId()).orElse(null);

    switch (event.getEventType()) {
        case "ORDER_PLACED":
            if (order == null) {
                orderRepo.save(new Order(event.getOrderId(), OrderStatus.PLACED));
            }
            break;

        case "ORDER_DELIVERED":
            if (order == null || order.getStatus() != OrderStatus.SHIPPED) {
                // Out-of-order event detected!
                log.warn("Received DELIVERED before SHIPPED for order {}. Parking event in retry topic.",
                         event.getOrderId());
                // Publish to a delayed retry topic or hold in Redis pending queue
                delayedRetryService.scheduleRetry(event, Duration.ofSeconds(10));
                return;
            }
            order.setStatus(OrderStatus.DELIVERED);
            orderRepo.save(order);
            break;
    }
}
```

---

## 🗜️ Scenario 7: Compacted Topics (Latest State Preservation)

> **Interviewer asks:** "We maintain user profile updates in a Kafka topic. Downstream analytics only ever cares about the latest profile for each user, not the history of 100 updates made over the last 3 years. How do you configure and maintain a log-compacted topic? What are tombstones, and why might old values still linger?"

### 1. How Log Compaction Works Internally

Kafka separates log segments into two sections:
- **Tail (Clean):** Segments already compacted. Contains only the latest record for each key.
- **Head (Dirty):** Newly appended records. May contain multiple duplicate keys.
- **Active Segment:** The current segment actively receiving writes. **The active segment is NEVER compacted**, regardless of configuration!

```
LOG SEGMENT COMPACTION PROCESS:
Before Compaction:
[k1:v1][k2:v1][k1:v2][k3:v1][k2:v2][k1:v3] (Active Segment: [k4:v1])
                         │
                         ▼ Log Cleaner Thread
After Compaction:
[k3:v1][k2:v2][k1:v3]                      (Active Segment remains untouched)
```

---

### 2. Production Configuration for Compacted Topics

```bash
kafka-topics.sh --bootstrap-server localhost:9092 --create \
  --topic user-profiles \
  --partitions 12 \
  --replication-factor 3 \
  --config cleanup.policy=compact \
  --config min.cleanable.dirty.ratio=0.5 \
  --config segment.ms=86400000 \
  --config segment.bytes=1073741824 \
  --config delete.retention.ms=86400000 \
  --config min.compaction.lag.ms=0
```

#### Key Parameter Deep Dive:
| Parameter | Value | Critical Purpose |
|---|---|---|
| `cleanup.policy` | `compact` | Enables compaction instead of standard time-based segment deletion (`delete`). |
| `min.cleanable.dirty.ratio` | `0.5` | Cleaner thread runs when dirty records make up $\ge 50\%$ of the log segment. |
| `segment.ms` | `86400000` (24h) | Forces active segment to roll over after 24h even if 1GB limit isn't reached, allowing compaction to clean it. |
| `delete.retention.ms` | `86400000` (24h) | Time a **tombstone** is preserved so downstream consumers can observe the deletion. |
| `min.compaction.lag.ms` | `0` | Minimum time a message remains uncompacted in head. |

---

### 3. Deleting Data: The Tombstone Pattern
To delete a key in a compacted topic, the producer sends a record with the key and a **`null` value**:

```java
// Producing a Tombstone message to delete user_987
ProducerRecord<String, UserProfile> tombstone = 
    new ProducerRecord<>("user-profiles", "user_987", null);
producer.send(tombstone);
```

#### Why Old Values Still Linger (Common Interview Traps):
1. **Records in Active Segment:** Compaction never touches the currently active log segment.
2. **Dirty Ratio Not Reached:** If `min.cleanable.dirty.ratio = 0.5` and dirty data is only 20%, compaction won't trigger.
3. **`delete.retention.ms` Not Expired:** Tombstones are intentionally kept alive to give slow consumers time to read the null value and delete the local database record.

---

## ⚡ Scenario 8: Producer Throughput Scaling (200 to 50,000 msg/s)

> **Interviewer asks:** "A telemetry producer application is stuck at 200 messages/second. CPU and network utilization on the producer host are under 5%. How do you diagnose the bottleneck and tune the producer to achieve 50,000 messages/second?"

### 1. Root Cause: Synchronous Blocking Calls
If a producer does this:
```java
// ANTI-PATTERN:
for (TelemetryEvent event : events) {
    producer.send(record).get(); // Synchronous block waiting for ACK round-trip!
}
```
If network Round Trip Time (RTT) between producer and broker is 5ms:
$$\text{Max Throughput} = \frac{1000\text{ ms}}{5\text{ ms}} = 200\text{ records/second!}$$
The producer is spending 99% of its time idling waiting for network acknowledgments.

---

### 2. High-Throughput Production Tuning Playbook

```
┌────────────────────────────────────────────────────────────────────────┐
│                      HIGH-THROUGHPUT PIPELINE                          │
│                                                                        │
│   producer.send(record, callback)  <-- ASYNC NON-BLOCKING              │
│               │                                                        │
│               ▼                                                        │
│   RecordAccumulator (batch.size = 128KB, linger.ms = 20ms)             │
│               │                                                        │
│               ▼ Compression (LZ4 / ZSTD: 70% smaller packets)          │
│               │                                                        │
│               ▼ Sender Thread (max.in.flight.requests = 5)             │
│               │                                                        │
│               ▼ Socket Buffer (send.buffer.bytes = 1MB)                │
└────────────────────────────────────────────────────────────────────────┘
```

#### Production Configuration Comparison:

| Parameter | Default Value | Tuned for 50k msg/s | Architectural Rationale |
|---|---|---|---|
| **Dispatch Mode** | Synchronous `.get()` | **Asynchronous with Callback** | Unblocks producer thread immediately. |
| `batch.size` | 16,384 (16KB) | **131,072 (128KB)** | Packs thousands of records into each network frame. |
| `linger.ms` | 0 | **20** | Waits up to 20ms to allow batches to fill to 128KB before transmitting. |
| `compression.type` | `none` | **`lz4`** (or `zstd`) | High-speed CPU compression reduces network I/O by 60-80%. |
| `buffer.memory` | 33,554,432 (32MB) | **67,108,864 (64MB)** | Prevents producer buffer exhaustion during spikes. |
| `max.in.flight.requests`| 5 | **5** | Allows 5 concurrent batches on the wire per connection. |
| `acks` | `all` | **`all` (with idempotence)** | Guarantees reliability without sacrificing batch throughput. |
| `send.buffer.bytes` | 131,072 (128KB) | **1,048,576 (1MB)** | Enlarges OS TCP socket buffer for gigabit networks. |

#### Tuned Java Producer Code:
```java
Properties props = new Properties();
props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9092");
props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

// 1. Throughput & Batching Settings
props.put(ProducerConfig.BATCH_SIZE_CONFIG, 128 * 1024);     // 128 KB
props.put(ProducerConfig.LINGER_MS_CONFIG, 20);              // 20 ms wait
props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");    // Low CPU, high speed
props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 64 * 1024 * 1024); // 64 MB buffer

// 2. High-Performance Reliability Settings
props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
props.put(ProducerConfig.ACKS_CONFIG, "all");
props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props);

// 3. Asynchronous Dispatch Loop
for (byte[] payload : highVolumeStream) {
    producer.send(new ProducerRecord<>("telemetry-topic", payload), (metadata, exception) -> {
        if (exception != null) {
            log.error("Failed to send record async", exception);
        }
    });
}
```

---

## 💥 Scenario 9: Handling Downstream Service Outages

> **Interviewer asks:** "Our Kafka consumer calls a downstream 3rd-party Payment Gateway. The gateway experiences an outage. Our consumer’s retry logic hammers the dead gateway, exhausting thread pools, missing Kafka heartbeats, and causing cascading rebalance storms. What architecture prevents this death spiral?"

### 1. Root Cause: The Cascading Retry Death Spiral
```
Downstream Gateway DOWN
          │
          ▼
Consumer thread blocks for 30s on HTTP connection timeout
          │
          ▼
Multiple threads block -> poll() is delayed beyond max.poll.interval.ms
          │
          ▼
Group Coordinator assumes consumer is DEAD -> Evicts consumer from group
          │
          ▼
STOP-THE-WORLD CONSUMER REBALANCE triggered across all other healthy instances!
```

---

### 2. Production Resilience Architecture

```
┌────────────────────────────────────────────────────────────────────────┐
│               NON-BLOCKING RETRY TOPICS & CIRCUIT BREAKER               │
│                                                                        │
│   Topic: `orders` ──────────────────────────────────────────┐          │
│         │ (Failure: HTTP 503)                               │          │
│         ▼                                                   │          │
│   Topic: `orders-retry-1m` (Delay: 1 min)                   │          │
│         │ (Failure: HTTP 503)                               ▼          │
│         ▼                                           CIRCUIT BREAKER    │
│   Topic: `orders-retry-5m` (Delay: 5 min)              (OPEN)          │
│         │ (Failure: HTTP 503)                               │          │
│         ▼                                                   ▼          │
│   Topic: `orders-dlt` (Dead Letter Topic)           consumer.pause()   │
│         │                                           Stop polling topic │
│         ▼                                           Keep heartbeat alive
│   Alert Slack / PagerDuty                                              │
└────────────────────────────────────────────────────────────────────────┘
```

#### Pillar 1: Circuit Breaker with `consumer.pause()`
When downstream failure rate exceeds 50%, the circuit opens. Instead of crashing, the consumer calls `consumer.pause(partitions)`:
- It continues calling `consumer.poll(Duration.ofMillis(100))` in a lightweight loop to return zero records while maintaining active broker **heartbeats**.
- No rebalance occurs!
- When the circuit breaker detects downstream recovery (Half-Open trial call succeeds), it calls `consumer.resume(partitions)`.

```java
@Component
public class ResilientConsumer {

    @Autowired
    private CircuitBreakerRegistry circuitBreakerRegistry;

    @KafkaListener(topics = "payment-requests", groupId = "payment-group")
    public void process(ConsumerRecord<String, PaymentRequest> record, Acknowledgment ack, Consumer<?, ?> consumer) {
        CircuitBreaker cb = circuitBreakerRegistry.circuitBreaker("paymentGateway");

        if (!cb.tryAcquirePermission()) {
            // Circuit is OPEN! Pause consumption to prevent hammering downstream
            log.warn("Downstream gateway is DOWN. Pausing consumer partitions...");
            consumer.pause(consumer.assignment());
            scheduleResume(consumer, Duration.ofSeconds(30));
            return;
        }

        try {
            callDownstreamPaymentGateway(record.value());
            cb.onSuccess(0, TimeUnit.MILLISECONDS);
            ack.acknowledge();
        } catch (Exception ex) {
            cb.onError(0, TimeUnit.MILLISECONDS, ex);
            throw ex; // Triggers Spring Kafka non-blocking retry topic routing
        }
    }
}
```

#### Pillar 2: Non-Blocking Progressive Retry Topics
In Spring Kafka 2.7+, configure `@RetryableTopic`:
```java
@RetryableTopic(
    attempts = "4",
    backoff = @Backoff(delay = 60000, multiplier = 3.0, maxDelay = 600000),
    dltTopicSuffix = "-dlt",
    autoCreateTopics = "true",
    include = { RestClientException.class, TimeoutException.class }
)
@KafkaListener(topics = "orders")
public void handleOrder(Order order) {
    paymentService.callGateway(order);
}
```

---

## 📍 Scenario 10: Live Location Tracking System at Scale

> **Interviewer asks:** "Design a real-time driver tracking pipeline for Uber/Lyft with 1 million active drivers. Each driver emits GPS coordinates every 2 seconds. How do you partition, tune, and size this Kafka cluster to handle the load reliably with sub-second latency?"

### 1. Capacity & Math Calculations

$$\text{Active Drivers} = 1,000,000$$
$$\text{Emission Frequency} = \text{1 event every 2 seconds}$$
$$\text{Throughput} = \frac{1,000,000}{2} = \mathbf{500,000\text{ writes/second}}$$

$$\text{Payload per GPS Ping (Protobuf)} = \sim 150\text{ bytes}$$
$$\text{Ingress Bandwidth} = 500,000 \times 150\text{ bytes} = 75,000,000\text{ bytes/sec} \approx \mathbf{75\text{ MB/sec (600 Mbps)}}$$

---

### 2. End-to-End Architecture

```
[1,000,000 Drivers Mobile App]
            │ (gRPC / WebSocket over TLS)
            ▼
┌──────────────────────────────────────┐
│  Edge API Gateways (Envoy / Netty)   │  Terminates connections, encodes Protobuf
└──────────────────┬───────────────────┘
                   │ Kafka Producer (linger.ms=20, acks=1, lz4)
                   ▼
┌────────────────────────────────────────────────────────────────────────┐
│                         KAFKA CLUSTER (6 BROKERS)                      │
│                                                                        │
│   Topic: `driver-locations`                                            │
│   - Partitions: 100 (Each partition handles ~5,000 events/sec)        │
│   - Key: `driver_id` (Preserves chronological trajectory per driver)   │
│   - Retention: 2 hours (Ephemeral in-memory page cache)                │
└──────────────────┬─────────────────────────────────────────────────────┘
                   │
         ┌─────────┴────────────────────────┐
         ▼                                  ▼
┌─────────────────────────┐      ┌─────────────────────────┐
│ KAFKA STREAMS / FLINK   │      │ TILE38 / REDIS CLUSTER  │
│ - Geo-fencing alerts    │      │ - Latest GPS per driver │
│ - Surge pricing metrics │      │ - Geospatial search API │
└─────────────────────────┘      └─────────────────────────┘
```

---

### 3. Key Design Choices & Trade-Offs

#### 1. Partitioning Strategy: `driver_id` vs `geohash`
- **Choice: Partition by `driver_id`**
  - **Reason:** Drivers constantly move across geographic boundaries. Partitioning by geohash causes a single driver's updates to hop across partitions, destroying order of movement and calculating invalid speeds!
  - Partitioning by `driver_id` guarantees that all pings for Driver #4410 go to the exact same partition in strict sequential order.

#### 2. Partition Sizing:
- Standard Kafka partition comfortably handles **5 MB/sec or ~5,000 records/sec**.
- At 500,000 records/sec:
  $$\text{Partitions} = \frac{500,000}{5,000} = \mathbf{100\text{ partitions}}$$

#### 3. Producer Optimization for Ephemeral Telemetry:
```properties
acks=1                         # Ultra-fast ACK from partition leader (0.1% loss acceptable for GPS)
compression.type=lz4           # Minimal CPU impact on mobile gateways
linger.ms=20                   # Micro-batching window
batch.size=65536               # 64 KB
```

#### 4. Retention Policy:
GPS locations are transient; historical pings older than 2 hours are useless for real-time dispatch:
```properties
retention.ms=7200000           # 2 hours
log.roll.ms=1800000            # Roll segments every 30 minutes
```

---

## 🔍 Scenario 11: Data Retention & Recovery Investigation

> **Interviewer asks:** "A newly onboarded Data Science team needs to ingest raw transaction events from 3 days ago. The original dev team claims: 'We have `log.retention.hours = 168` (7 days) in `server.properties`, but when we seek to 3 days ago, Kafka returns empty records — the data is gone.' Is that possible? How do you investigate what happened?"

### 1. Root Cause Investigation Checklist

Even if default `log.retention.hours = 168` is configured on the broker, data can be purged prematurely due to **five hidden causes**:

```
                              DATA PURGE INVESTIGATION
                                         │
        ┌───────────────────┬────────────┴───────┬────────────────────┐
        ▼                   ▼                    ▼                    ▼
Topic-Level Override   Size-Based Purge    Log Compaction      Future Timestamp Bug
`retention.ms` set to  `retention.bytes`   `cleanup.policy`   Segment rolled and
24h on topic metadata  limit reached       compacted away old  deleted immediately
```

1. **Topic-Level Configuration Override:**
   - Topic-level configs take precedence over broker `server.properties`. Someone might have created the topic with `retention.ms = 86400000` (24 hours).
2. **Size-Based Purge (`log.retention.bytes` or `retention.bytes`):**
   - Kafka purges whichever threshold is hit **first**: time OR size.
   - If `retention.bytes = 10737418240` (10GB) and high traffic produced 10GB in 18 hours, old segments are deleted after 18 hours despite the 7-day time setting!
3. **Log Compaction Enabled:**
   - If `cleanup.policy = compact`, older records with duplicate keys are deleted by the log cleaner.
4. **Consumer Offset Expiration (`offsets.retention.minutes`):**
   - The data is actually still on disk in Kafka! However, the consumer group’s committed offset metadata expired after 7 days of inactivity. When the consumer started with `auto.offset.reset = latest`, it jumped directly to the end of the topic, making it *appear* empty.

---

### 2. Diagnostic Command Toolkit

#### Step 1: Inspect Topic Metadata Overrides
```bash
kafka-configs.sh --bootstrap-server localhost:9092 \
  --entity-type topics --entity-name transaction-events \
  --describe
```
*Look for:* `retention.ms`, `retention.bytes`, or `cleanup.policy` overrides.

#### Step 2: Check Earliest Available Offset on Partitions
```bash
# Check earliest (-2) and latest (-1) offsets
kafka-run-class.sh kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:9092 \
  --topic transaction-events \
  --time -2
```
If the earliest offset returns a number whose timestamp is from 3 days ago, **the data is still intact!**

#### Step 3: Inspect Actual Physical Segment Files on Broker Disk
```bash
ls -lh /var/lib/kafka/data/transaction-events-0/
```
Check the timestamps of the `.log` files. If files dated 3 days ago exist, the issue is purely consumer configuration (`auto.offset.reset`).

---

## ⚠️ Scenario 12: Consumer Save Failure (DB OK, Offset Commit Fails)

> **Interviewer asks:** "A consumer receives a message, successfully inserts the row into Postgres, but before `commitSync()` reaches Kafka, the network drops or the consumer pod is killed. When the consumer recovers, Kafka delivers the message a second time. How do you prevent duplicate processing?"

### 1. Root Cause: Distributed Dual-State Failure
In a non-distributed transaction, saving state to an external database and committing an offset to Kafka are two distinct I/O operations. Since Kafka offset commit is not part of the database transaction, network failure between step 1 and step 2 creates **at-least-once duplicate delivery**.

```
[Poll Record (ID: 501)]
          │
          ▼
1. INSERT INTO orders (id=501) ───► [SUCCESS in PostgreSQL]
          │
2. consumer.commitSync() ─────────► [NETWORK BREAK / CRASH]
          │
          ▼
[Pod Restarts]
Kafka redelivers Record (ID: 501)!
```

---

### 2. Solutions to Guarantee Exactly-Once Business Processing

#### Solution A: The Idempotent Database Consumer (Recommended)
Make the database write operation naturally idempotent so duplicate processing produces zero side-effects.

```sql
-- 1. Unique Constraint on Business Identifier
CREATE TABLE processed_orders (
    order_id VARCHAR(64) PRIMARY KEY,
    customer_id VARCHAR(64) NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Atomic Upsert or Ignore
INSERT INTO processed_orders (order_id, customer_id, amount)
VALUES (:orderId, :customerId, :amount)
ON CONFLICT (order_id) DO NOTHING;
```

```java
@Transactional
public void processRecordIdempotent(OrderDto order) {
    int rowsInserted = orderRepository.insertIfNotExists(order.getId(), order.getAmount());
    if (rowsInserted == 0) {
        log.info("Duplicate record {} detected. Skipping side-effects.", order.getId());
        return; // Safe!
    }
    // Execute one-time side-effects (e.g. loyalty points)
    loyaltyService.grantPoints(order.getCustomerId(), order.getAmount());
}
```

---

#### Solution B: Store Kafka Offsets Inside the Database Transaction
If strict exactly-once database consistency is required without relying on external idempotency keys, store the **Kafka offset inside the database itself**:

```sql
CREATE TABLE kafka_consumer_offsets (
    consumer_group VARCHAR(100) NOT NULL,
    topic VARCHAR(100) NOT NULL,
    partition INT NOT NULL,
    committed_offset BIGINT NOT NULL,
    PRIMARY KEY (consumer_group, topic, partition)
);
```

```java
@Transactional
public void processAndCommitAtomically(ConsumerRecord<String, String> record) {
    // 1. Check current stored offset in DB
    long lastOffset = offsetRepo.getOffset("order-group", record.topic(), record.partition());
    if (record.offset() <= lastOffset) {
        return; // Already processed!
    }

    // 2. Perform business mutation
    orderRepo.saveOrder(parse(record.value()));

    // 3. Update offset in the exact same DB transaction
    offsetRepo.updateOffset("order-group", record.topic(), record.partition(), record.offset());
    
    // DB Commits both together atomically!
}
```
On consumer startup or partition rebalance, invoke `consumer.seek(partition, dbOffset + 1)` using `ConsumerRebalanceListener`.

---

## 💾 Scenario 13: Broker Disk Space Sudden Exhaustion

> **Interviewer asks:** "At 3:00 AM, Broker 3’s disk filled up to 100%, causing the broker to crash. No traffic spikes were observed, and global retention is set to 7 days. What could cause a broker's disk to suddenly fill up overnight without configuration changes?"

### 1. Root Cause Breakdown

```
                       BROKER DISK FULL INCIDENT
                                   │
        ┌──────────────────────────┼──────────────────────────┐
        ▼                          ▼                          ▼
Future Timestamp Bug        Log Cleaner Dead            Corrupted Index /
(Endless Segment Rolling)   (Compaction Silent Freeze)  Unbalanced Partition Rebalance
```

#### Culprit 1: The "Future Timestamp" Segment Rolling Bug
- Kafka creates a new log segment file when:
  1. Segment reaches `segment.bytes` (e.g., 1 GB), OR
  2. The timestamp difference between the current message and the first message in the segment exceeds `segment.ms`.
- **The Disaster:** A bug in an upstream mobile client or producer sent records with timestamps set to the year **2099** or epoch `Long.MAX_VALUE`.
- Every incoming record with a future timestamp causes Kafka to think the current segment has exceeded its time window, **rolling a brand new 1GB-allocated segment for every single message!** Millions of `.log` and `.index` files are created within hours, exhausting disk space and OS inodes.

#### Culprit 2: Log Cleaner Thread Died Silently (OOM / Corruption)
- On compacted topics, if a segment file is corrupted or an `OutOfMemoryError` occurs in the `LogCleanerThread`, the thread **dies silently**.
- Writes continue to accumulate in dirty head segments, but compaction never runs. Disk usage climbs continuously until exhaustion.

#### Culprit 3: Cluster Rebalance or Controller Reassignment Spike
- A partition reassignment command or Cruise-Control auto-rebalance moved 50 heavy partitions onto Broker 3 simultaneously without disk throttling.

---

### 2. Emergency Remediation & Prevention Runbook

#### Emergency Remediation:
```bash
# 1. Identify which topic partition folder is consuming disk
du -sh /var/lib/kafka/data/* | sort -hr | head -n 10

# 2. Check for dead log cleaner threads in broker log
grep -i "Dead log cleaner" /var/log/kafka/server.log

# 3. Dynamically lower retention on the rogue topic to free space immediately
kafka-configs.sh --bootstrap-server broker1:9092 \
  --entity-type topics --entity-name rogue-topic \
  --alter --add-config retention.ms=3600000
```

#### Long-Term Prevention Settings:
Reject future timestamps on the broker:
```properties
# Reject messages with timestamps exceeding 1 hour into the future
log.message.timestamp.type=CreateTime
log.message.timestamp.difference.max.ms=3600000
```

---

## 👥 Scenario 14: Consumer Performance Blast Radius

> **Interviewer asks:** "We have 5 independent microservices reading from the same `orders` topic. If Service C’s consumer becomes extremely slow due to database locking, will it impact or degrade the performance of the other 4 services?"

### 1. Core Architectural Distinction

The answer depends completely on whether the 5 services share the **same Consumer Group** or use **different Consumer Groups**:

```
                       TOPIC: `orders` (Partitions: 0, 1, 2, 3)
                                      │
            ┌─────────────────────────┴────────────────────────┐
            ▼                                                  ▼
[SCENARIO A: SAME GROUP]                          [SCENARIO B: INDEPENDENT GROUPS]
Consumer Group: `order-fulfillment`                Group A: `billing`
├── Pod 1 (Fast) -> Part 0                         ├── Pod 1 -> All Partitions
├── Pod 2 (Fast) -> Part 1                         Group B: `analytics`
├── Pod 3 (SLOW) -> Part 2 [BLAST RADIUS!]         ├── Pod 2 -> All Partitions
└── Pod 4 (Fast) -> Part 3                         Group C: `inventory` (SLOW)
                                                   └── Pod 3 -> All Partitions
```

---

### 2. Scenario A: Same Consumer Group (`group.id = 'fulfillment'`)
**Impact: SEVERE & CATASTROPHIC.**
1. **Partition Lag Isolation:** Only the partitions assigned to Pod 3 will accumulate lag directly.
2. **The Rebalance Domino Effect:**
   - If Pod 3 takes longer than `max.poll.interval.ms` (e.g., 5 minutes) to process a batch, the Kafka coordinator declares Pod 3 **dead**.
   - Coordinator triggers a **Consumer Group Rebalance**.
   - During eager rebalancing, **ALL 5 pods are paused (Stop-The-World)**.
   - When Pod 3 revives and rejoins, another rebalance is triggered, causing an endless **Rebalance Loop** that halts processing for everyone!

---

### 3. Scenario B: Different Consumer Groups (`group.id` distinct per service)
**Logical Impact: NONE.**
- Kafka maintains offset tracking completely independently per `group.id` in `__consumer_offsets`.
- Service C falling 5 million messages behind does not logically stall Service A or Service B.

**Physical Broker Impact: POTENTIAL DISK I/O DEGRADATION (Cache Thrashing).**
- **The Page Cache Trap:** Healthy, fast consumers read messages from the Linux kernel’s **OS Page Cache (RAM)** with zero disk I/O.
- When Service C falls hours behind, its reads fall outside the Page Cache.
- The broker must perform **random physical disk reads** to retrieve old segments from disk.
- This saturates broker disk read heads and NIC bandwidth, driving up latency and reducing throughput for **all** consumers hosted on that broker!

---

## 📊 Summary Matrix: Quick Scenario Cheat Sheet

| # | Scenario | Core Root Cause | Best Architectural Solution |
|---|---|---|---|
| **1** | Payment Dual-Write | Distributed consistency failure | **Transactional Outbox Pattern + Debezium CDC** |
| **2** | 200 Orders Lost Silently | `enable.auto.commit=true` before processing | **`AckMode.MANUAL_IMMEDIATE` + Synchronous DB commit** |
| **3** | Adding Consumers Fails (5M Lag) | Partitions < Consumers (Partition ceiling) | **Parallel Consumer / Virtual Threads OR alter partitions** |
| **4** | Celebrity Hot Partition | Single key hashing to 1 partition | **Key Salting (`celebrity#salt`) or Dynamic Partitioner** |
| **5** | Re-processing 2 Hours | Need time rewind without duplicates | **`offsetsForTimes` + Idempotent Upserts (`ON CONFLICT`)** |
| **6** | Delivered Before Placed | Producer retries / Multi-partition keys | **`enable.idempotence=true`, single key, state machine validation** |
| **7** | Compacted Topic Retention | Active segment untouched / missing tombstones | **`cleanup.policy=compact`, `segment.ms`, tombstones (`null` value)** |
| **8** | Producer Stuck at 200 msg/s | Synchronous `.get()` blocking RTT | **Async dispatch + `linger.ms=20` + `batch.size=128KB` + `lz4`** |
| **9** | Downstream Service Outage | Synchronous retry thread exhaustion | **Circuit Breaker `pause()` + Non-blocking retry topics** |
| **10**| 1M Drivers GPS Tracking | High write ingress (500k/s) | **100 partitions, key by `driver_id`, `linger.ms=20`, `acks=1`** |
| **11**| 3-Day Data Missing | Size-based limit / override / offset expiry | **Check `retention.bytes`, `GetOffsetShell`, check disk segments** |
| **12**| DB Saved, Offset Commit Fails | Dual-state failure on redelivery | **Idempotent consumer table constraint OR offset in DB** |
| **13**| Sudden Broker Disk Full | Future timestamp segment rolling / dead cleaner | **Reject future timestamps (`difference.max.ms`), monitor cleaner** |
| **14**| 1 Slow Consumer Impact | Rebalance storms (same group) / Page cache thrash (diff group) | **Tune `max.poll.interval.ms`, use `CooperativeStickyAssignor`** |
