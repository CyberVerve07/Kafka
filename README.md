# 🚀 Kafka Learning Repository

> A comprehensive, structured guide to mastering Apache Kafka — from core architecture to Spring Boot integration and real-world interview preparation.

---

## 📁 Repository Structure

```
Kafka/
├── 📂 01-Kafka-Architecture/          # Deep-dive into Kafka internals & concepts
│   ├── 01-Introduction/
│   ├── 02-Core-Concepts/
│   ├── 03-Producers/
│   ├── 04-Consumers/
│   ├── 05-Topics-and-Partitions/
│   ├── 06-Brokers-and-Clusters/
│   ├── 07-Zookeeper-and-KRaft/
│   ├── 08-Replication-and-Fault-Tolerance/
│   ├── 09-Kafka-Streams/
│   ├── 10-Kafka-Connect/
│   └── README.md
│
├── 📂 02-Java-SpringBoot-Kafka/        # Hands-on Spring Boot + Kafka code
│   ├── 01-Setup-and-Configuration/
│   ├── 02-Simple-Producer-Consumer/
│   ├── 03-Custom-Serializer-Deserializer/
│   ├── 04-Kafka-Listener-Patterns/
│   ├── 05-Error-Handling-and-Retry/
│   ├── 06-Transactions/
│   ├── 07-Kafka-Streams-with-Spring/
│   ├── 08-Real-World-Scenarios/
│   └── README.md
│
└── 📂 03-Kafka-Interview-Questions/    # Scenario-based interview prep
    ├── 01-Beginner-Level/
    ├── 02-Intermediate-Level/
    ├── 03-Advanced-Level/
    ├── 04-Scenario-Based/
    ├── 05-System-Design/
    ├── 06-Extreme-Hard-Scenarios/
    └── README.md
```

---

## 🎯 Learning Path

```
Beginner → Kafka Architecture → Spring Boot Code → Interview Prep
```

| Stage | Folder | Goal |
|-------|--------|------|
| 🟢 Beginner | `01-Kafka-Architecture` | Understand how Kafka works |
| 🟡 Intermediate | `02-Java-SpringBoot-Kafka` | Build real applications |
| 🔴 Advanced | `03-Kafka-Interview-Questions` | Ace interviews with confidence |

---

## 🏗️ High-Level Kafka Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        KAFKA ECOSYSTEM                          │
│                                                                 │
│  ┌──────────┐    ┌─────────────────────────────────────────┐   │
│  │PRODUCERS │───▶│              KAFKA CLUSTER              │   │
│  │          │    │  ┌─────────┐ ┌─────────┐ ┌─────────┐   │   │
│  │ App1     │    │  │ Broker1 │ │ Broker2 │ │ Broker3 │   │   │
│  │ App2     │    │  │ Topic-A │ │ Topic-A │ │ Topic-A │   │   │
│  │ App3     │    │  │ Part-0  │ │ Part-1  │ │ Part-2  │   │   │
│  └──────────┘    │  └─────────┘ └─────────┘ └─────────┘   │   │
│                  └─────────────────────────────────────────┘   │
│                                       │                        │
│                              ┌────────▼────────┐               │
│                              │   ZOOKEEPER /   │               │
│                              │     KRaft       │               │
│                              └────────┬────────┘               │
│                                       │                        │
│  ┌──────────┐    ┌────────────────────▼────────────────────┐   │
│  │CONSUMERS │◀───│           CONSUMER GROUPS               │   │
│  │          │    │  Group-A: [Consumer1, Consumer2]        │   │
│  │ Service1 │    │  Group-B: [Consumer3]                   │   │
│  │ Service2 │    └─────────────────────────────────────────┘   │
│  └──────────┘                                                   │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Tools & Technologies

| Tool | Purpose |
|------|---------|
| Apache Kafka | Message streaming platform |
| Java 17+ | Primary language |
| Spring Boot 3.x | Application framework |
| Spring Kafka | Kafka integration |
| Docker | Local Kafka setup |
| Zookeeper / KRaft | Cluster coordination |

---

## 📖 How to Use This Repo

1. **Start** with `01-Kafka-Architecture` to build conceptual foundations
2. **Code along** with `02-Java-SpringBoot-Kafka` for hands-on practice
3. **Practice** with `03-Kafka-Interview-Questions` before interviews

---

> 💡 *"Kafka is not just a message queue — it's a distributed commit log that powers real-time data pipelines."*
