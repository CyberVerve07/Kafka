# 🎭 Scenario-Based Interview Questions (Top Company Favorites)

---

## Scenario 1: Messages processed out of order in a Banking App
> **Interviewer asks:** "We process money transfers. Step 1: Deposit, Step 2: Withdrawal. Sometimes withdrawal executes before deposit. How to fix?"

### Solution:
1. **Root cause:** Messages without keys or with different keys went to different partitions or were consumed by different consumer threads.
2. **Fix:**
   - Use `account_id` as the Kafka message **Key**.
   - Messages with the same key always go to the **same partition**.
   - Inside partition, Kafka guarantees strict FIFO ordering.
   - Set producer `max.in.flight.requests.per.connection=1` (or `5` if idempotence is enabled).

---

## Scenario 2: Handling a "Poison Pill" message
> **Interviewer asks:** "A corrupted JSON message causes the consumer to throw an exception repeatedly. Consumer is stuck in an infinite retry loop. How do you resolve this in production?"

### Solution:
1. **Dead Letter Topic (DLT)** pattern with `DefaultErrorHandler`:
   - Set max retry limit (e.g., 3 attempts).
   - If error persists, publish to `topic-name.DLT`.
   - Consumer commits offset and moves forward to the next message.
2. **Alerting & Fix:**
   - DLT consumer stores corrupted message in DB and triggers Slack/PagerDuty alert for investigation.

---

## Scenario 3: High Consumer Lag during Flash Sale
> **Interviewer asks:** "During a flash sale, order consumer lag jumps to 5 million messages. How do you scale?"

### Solution:
1. Check partition count: If topic has 10 partitions and 10 consumers, adding more consumers in the same group won't help!
2. Increase partitions dynamically (`kafka-topics.sh --alter --partitions 30`).
3. Scale consumer deployment to 30 replicas.
4. If partition increase breaks key-ordering or is not immediate: Implement an in-memory worker thread pool inside consumer instances.
