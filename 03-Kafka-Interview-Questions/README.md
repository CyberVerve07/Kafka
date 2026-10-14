# 🎯 Kafka Interview Questions — Scenario Based

> Comprehensive interview preparation covering beginner to advanced Kafka concepts, with real-world scenario-based questions that top companies actually ask.

---

## 📁 Folder Structure

```
03-Kafka-Interview-Questions/
│
├── 01-Beginner-Level/
│   └── README.md          ← What is Kafka? Basic concepts Q&A
│
├── 02-Intermediate-Level/
│   └── README.md          ← Partitions, consumer groups, offsets
│
├── 03-Advanced-Level/
│   └── README.md          ← Exactly-once, ISR, performance tuning
│
├── 04-Scenario-Based/
│   └── README.md          ← Real-world problem scenarios ★ Most Important
│
└── 05-System-Design/
    └── README.md          ← Design Kafka-based systems from scratch
```

---

## 📊 Question Distribution

```
  ┌─────────────────────────────────────────────────────────────┐
  │               INTERVIEW QUESTION CATEGORIES                 │
  │                                                             │
  │  🟢 Beginner (20 Q)     ████████░░░░░░░░░░  ~25%           │
  │  🟡 Intermediate (25 Q) ██████████░░░░░░░░  ~30%           │
  │  🔴 Advanced (20 Q)     ████████░░░░░░░░░░  ~25%           │
  │  🎭 Scenario-Based (15) ██████░░░░░░░░░░░░  ~15%           │
  │  🏗️ System Design (5)   ██░░░░░░░░░░░░░░░░  ~5%            │
  │                                                             │
  │  Total: ~85 Questions                                       │
  └─────────────────────────────────────────────────────────────┘
```

---

## 🏆 Most Asked Topics By Company

```
  ┌─────────────────────────────────────────────────────────────┐
  │  Company          │ Top Topics They Ask                     │
  │─────────────────────────────────────────────────────────────│
  │  Amazon/AWS       │ Replication, MSK, Exactly-Once          │
  │  Netflix          │ Consumer lag, Performance tuning        │
  │  Uber             │ Partitioning strategy, ordering          │
  │  LinkedIn         │ Kafka internals (they built it!)        │
  │  Flipkart/Swiggy  │ Microservices + Kafka patterns          │
  │  Banks (JP, etc)  │ Transactions, Exactly-Once              │
  └─────────────────────────────────────────────────────────────┘
```

---

## 🎭 Scenario Decision Tree

```
  INTERVIEWER ASKS A SCENARIO QUESTION
              │
              ▼
    "What kind of problem is it?"
              │
    ┌─────────┼──────────┬──────────────┐
    │         │          │              │
  Ordering  Fault     Scaling       Latency
  Problem  Tolerance  Problem       Problem
    │         │          │              │
    ▼         ▼          ▼              ▼
  Talk     Talk       Talk         Talk about
  about    about      about        linger.ms,
  Keys &   Replica-   Partition    batch.size,
  Single   tion &     count &      compression
  Partition ISR       Consumer     settings
           setup      Groups
```

---

## ⭐ Top 10 Most Important Scenario Questions

| # | Scenario | Difficulty |
|---|----------|-----------|
| 1 | "Messages are being processed out of order — how do you fix it?" | 🟡 Medium |
| 2 | "A consumer is lagging millions of messages — what do you do?" | 🔴 Hard |
| 3 | "How do you ensure exactly-once delivery in a payment system?" | 🔴 Hard |
| 4 | "A broker goes down — what happens to your consumers?" | 🟡 Medium |
| 5 | "How would you design a real-time fraud detection system with Kafka?" | 🔴 Hard |
| 6 | "Your topic has hotspot partitions — how do you rebalance?" | 🔴 Hard |
| 7 | "How do you handle poison pill messages?" | 🟡 Medium |
| 8 | "Design an event-driven order system for an e-commerce platform" | 🔴 Hard |
| 9 | "How do you replay messages from a specific point in time?" | 🟢 Easy |
| 10 | "Kafka vs RabbitMQ — when would you choose which?" | 🟢 Easy |

---

## 📂 Navigate Each Section

| # | Section | Focus Area |
|---|---------|-----------|
| 01 | [Beginner Level](./01-Beginner-Level/README.md) | Definitions, basic concepts |
| 02 | [Intermediate Level](./02-Intermediate-Level/README.md) | Configuration, internals |
| 03 | [Advanced Level](./03-Advanced-Level/README.md) | Deep dive, edge cases |
| 04 | [Scenario Based ⭐](./04-Scenario-Based/README.md) | Real-world problem solving |
| 05 | [System Design](./05-System-Design/README.md) | Architecture design questions |

---

> 💡 **Pro Tip**: Interviewers love when you discuss **trade-offs**. Don't just say what to do — explain *why* and what you give up by choosing that approach.
