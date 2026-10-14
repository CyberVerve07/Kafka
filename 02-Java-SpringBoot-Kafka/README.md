# ☕ Java Spring Boot + Kafka — Hands-On Code

> Practical, runnable Spring Boot examples covering every Kafka pattern you'll use in production.

---

## 📁 Folder Structure

```
02-Java-SpringBoot-Kafka/
│
├── 01-Setup-and-Configuration/
│   ├── README.md               ← Docker setup, application.yml configs
│   └── docker-compose.yml      ← Local Kafka + Zookeeper
│
├── 02-Simple-Producer-Consumer/
│   ├── README.md               ← Basic producer/consumer example
│   ├── KafkaProducerService.java
│   └── KafkaConsumerService.java
│
├── 03-Custom-Serializer-Deserializer/
│   ├── README.md               ← JSON, Avro serialization
│   ├── OrderSerializer.java
│   └── OrderDeserializer.java
│
├── 04-Kafka-Listener-Patterns/
│   ├── README.md               ← @KafkaListener, batch, filter
│   ├── SimpleListener.java
│   ├── BatchListener.java
│   └── FilteredListener.java
│
├── 05-Error-Handling-and-Retry/
│   ├── README.md               ← DLT, retry, error handlers
│   ├── RetryConfig.java
│   └── DeadLetterTopicConfig.java
│
├── 06-Transactions/
│   ├── README.md               ← Exactly-once semantics
│   └── TransactionalProducer.java
│
├── 07-Kafka-Streams-with-Spring/
│   ├── README.md               ← KStream, KTable, aggregations
│   └── OrderStreamProcessor.java
│
└── 08-Real-World-Scenarios/
    ├── README.md               ← E-commerce order pipeline example
    ├── OrderProducer.java
    ├── InventoryConsumer.java
    └── NotificationConsumer.java
```

---

## 🏗️ Spring Boot + Kafka Architecture

```
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                    SPRING BOOT APPLICATION ARCHITECTURE                     │
  │                                                                             │
  │  ┌──────────────────────────────────────────────────────────────────────┐  │
  │  │                         REST API Layer                               │  │
  │  │            POST /orders ──────▶ OrderController                     │  │
  │  └──────────────────────────────────┬───────────────────────────────────┘  │
  │                                     │                                       │
  │  ┌──────────────────────────────────▼───────────────────────────────────┐  │
  │  │                        Service Layer                                 │  │
  │  │                       OrderService                                   │  │
  │  │                           │                                          │  │
  │  │              ┌────────────▼──────────────┐                           │  │
  │  │              │   KafkaProducerService    │                           │  │
  │  │              │   kafkaTemplate.send()    │                           │  │
  │  └──────────────┴────────────┬──────────────┴───────────────────────────┘  │
  │                              │                                              │
  └──────────────────────────────┼──────────────────────────────────────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │      KAFKA CLUSTER      │
                    │   Topic: "orders"       │
                    │   [P0] [P1] [P2]        │
                    └────────────┬────────────┘
                                 │
  ┌──────────────────────────────▼──────────────────────────────────────────────┐
  │                    CONSUMER MICROSERVICES                                    │
  │                                                                             │
  │  ┌─────────────────────┐  ┌─────────────────────┐  ┌────────────────────┐  │
  │  │  Inventory Service  │  │ Notification Service│  │  Analytics Service │  │
  │  │  @KafkaListener     │  │  @KafkaListener     │  │  Kafka Streams     │  │
  │  │  group: inventory   │  │  group: notify      │  │  group: analytics  │  │
  │  └─────────────────────┘  └─────────────────────┘  └────────────────────┘  │
  └─────────────────────────────────────────────────────────────────────────────┘
```

---

## ⚙️ Spring Kafka Configuration Overview

```yaml
# application.yml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
      acks: all                    # strongest guarantee
      retries: 3
      batch-size: 16384            # 16KB batching
      linger-ms: 5                 # wait 5ms to batch
    consumer:
      group-id: my-consumer-group
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      enable-auto-commit: false    # manual offset commit
    listener:
      ack-mode: MANUAL_IMMEDIATE
```

---

## 🔄 Error Handling Flow

```
  Message Arrives
       │
       ▼
  Process Message
       │
    Success? ──── YES ──── Commit Offset ──── Done ✅
       │
      NO
       │
       ▼
  Retry (1st attempt)
       │
    Success? ──── YES ──── Commit Offset ──── Done ✅
       │
      NO
       │
       ▼
  Retry (2nd attempt)
       │
    Success? ──── YES ──── Commit Offset ──── Done ✅
       │
      NO
       │
       ▼
  Send to Dead Letter Topic (DLT)
  Topic: "orders.DLT"
       │
       ▼
  Alert / Manual Intervention 🚨
```

---

## 📦 Dependencies (pom.xml)

```xml
<dependencies>
    <!-- Spring Boot Kafka Starter -->
    <dependency>
        <groupId>org.springframework.kafka</groupId>
        <artifactId>spring-kafka</artifactId>
    </dependency>

    <!-- Spring Boot Web -->
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-web</artifactId>
    </dependency>

    <!-- Lombok -->
    <dependency>
        <groupId>org.projectlombok</groupId>
        <artifactId>lombok</artifactId>
        <optional>true</optional>
    </dependency>

    <!-- Jackson for JSON -->
    <dependency>
        <groupId>com.fasterxml.jackson.core</groupId>
        <artifactId>jackson-databind</artifactId>
    </dependency>
</dependencies>
```

---

## 📂 Navigate Each Section

| # | Section | Key Concepts |
|---|---------|-------------|
| 01 | [Setup & Config](./01-Setup-and-Configuration/README.md) | Docker, YAML config |
| 02 | [Simple Producer/Consumer](./02-Simple-Producer-Consumer/README.md) | KafkaTemplate, @KafkaListener |
| 03 | [Serialization](./03-Custom-Serializer-Deserializer/README.md) | JSON, Avro, custom |
| 04 | [Listener Patterns](./04-Kafka-Listener-Patterns/README.md) | Batch, filter, partition |
| 05 | [Error Handling](./05-Error-Handling-and-Retry/README.md) | DLT, retry, SeekToCurrentErrorHandler |
| 06 | [Transactions](./06-Transactions/README.md) | Exactly-once, transactional ID |
| 07 | [Kafka Streams](./07-Kafka-Streams-with-Spring/README.md) | KStream, KTable, windowing |
| 08 | [Real World](./08-Real-World-Scenarios/README.md) | E-commerce order system |
