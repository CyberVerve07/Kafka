# 🛡️ Northguard — LinkedIn's Next-Generation Streaming Engine

> Northguard is LinkedIn's ground-up replacement for Apache Kafka's storage layer. It's written in C++, uses segment-level replication, and replaces Kafka's single controller with a Dynamically-Sharded Replicated State Machine (DS-RSM).

---

## 🏗️ The Big Picture: What Northguard Changes

```
  ┌────────────────────────────────────────────────────────────────────┐
  │                     APACHE KAFKA (Legacy)                          │
  │                                                                    │
  │   Topic → Partitions (monolithic, broker-bound, rigid)             │
  │   Metadata → Single ZooKeeper / KRaft controller                  │
  │   Replication → Full partition copied broker-to-broker             │
  │   Language → Java / Scala (JVM, GC pauses)                        │
  │   Rebalancing → Manual, disruptive, time-consuming                 │
  └────────────────────────────────────────────────────────────────────┘
                              ↓  replaced by  ↓
  ┌────────────────────────────────────────────────────────────────────┐
  │                      NORTHGUARD (New)                              │
  │                                                                    │
  │   Topic → Ranges → Segments (fine-grained, moveable, dynamic)     │
  │   Metadata → DS-RSM (sharded Raft vnodes, no single controller)   │
  │   Replication → 1 GB segments replicated independently             │
  │   Language → C++ (no GC, low latency, predictable performance)    │
  │   Rebalancing → Automatic self-balancing, no manual ops           │
  └────────────────────────────────────────────────────────────────────┘
```

---

## 📐 The Data Model: Records → Segments → Ranges → Topics

Northguard fundamentally reimagines how data is organized:

### Kafka's Data Model vs Northguard's Data Model

```
  KAFKA:
  Topic
  └── Partition (fixed count, immovable monolith)
      └── Segments (Kafka internal, 1 GB files, invisible to design)

  NORTHGUARD:
  Topic
  └── Ranges  (dynamic, splittable/mergeable keyspace slices)
      └── Segments (1 GB or 1 hour max, the atomic replication unit)
          └── Records (individual messages)
```

---

### 📦 Segments — The Atomic Unit

```
  ┌───────────────────────────────────────────────────────────┐
  │                    NORTHGUARD SEGMENT                     │
  │                                                           │
  │  Max Size:       1 GB                                     │
  │  Max Lifespan:   1 hour                                   │
  │  State:          Active (writable) or Sealed (immutable)  │
  │                                                           │
  │  When a segment fills (1 GB) or ages (1 hour):           │
  │  → It is SEALED (becomes immutable, never changes)        │
  │  → A new ACTIVE segment is created for the range          │
  │                                                           │
  │  Sealed segments:                                         │
  │  → Can be moved independently to other brokers            │
  │  → Can be replicated concurrently with other segments     │
  │  → Recovery = only recover missing segments               │
  │  → "Dead" broker → move its sealed segments, done         │
  └───────────────────────────────────────────────────────────┘
```

---

### 🔪 Ranges — Dynamic Keyspace Slices

```
  A Range is a contiguous slice of the keyspace for a topic.
  Unlike Kafka partitions (fixed at topic creation), ranges can:

  SPLIT:
  Range [A–Z] (getting hot, too much load)
      ↓
  Range [A–M]  +  Range [N–Z]
  (Two smaller ranges, each with their own active segment)
  → No data movement required
  → No global rebalance
  → Done automatically by the coordinator

  MERGE:
  Range [A–E]  +  Range [F–J]  (both cold/underutilized)
      ↓
  Range [A–J]
  → Reclaim resources
  → Reduce metadata overhead
```

---

### 📂 Topics — Collections of Ranges

```
  Topic: "user-activity-events"
  
  Range 0: keyspace [0x0000...0x3FFF]
  ├── Sealed Segment A (1 GB, stored on Broker 12)
  ├── Sealed Segment B (1 GB, stored on Broker 7)
  └── Active Segment C  (500 MB so far, Broker 3 is leader)
  
  Range 1: keyspace [0x4000...0x7FFF]
  ├── Sealed Segment D (1 GB, stored on Broker 5)
  └── Active Segment E  (200 MB so far, Broker 11 is leader)
  
  Range 2: keyspace [0x8000...0xFFFF]  ← (recently split from Range 1)
  └── Active Segment F  (50 MB so far, Broker 9 is leader)
```

---

## 🧠 The Control Plane: DS-RSM (Dynamically-Sharded Replicated State Machine)

This is Northguard's most critical innovation — the replacement for Kafka's single controller.

### The Problem with Kafka's Controller

```
  KAFKA:
  ┌──────────────────────────────────────────────────┐
  │   ONE CONTROLLER BROKER                          │
  │   → manages ALL partition state                  │
  │   → processes ALL metadata requests              │
  │   → broadcasts ALL updates to ALL brokers        │
  │   → single point of bottleneck                   │
  │   → single point of failure (failover = downtime)│
  └──────────────────────────────────────────────────┘
```

### DS-RSM: Sharded Coordination via Raft

```
  NORTHGUARD DS-RSM:

  ┌──────────────────────────────────────────────────────────────────┐
  │                   CONSISTENT HASH RING                           │
  │                                                                  │
  │   128+ vnodes distributed around the ring                       │
  │   Each vnode = an independent Raft group (3–5 replicas)         │
  │                                                                  │
  │      vnode_0  vnode_1  vnode_2 ... vnode_127                   │
  │        ↑        ↑        ↑              ↑                       │
  │      Raft     Raft     Raft           Raft                      │
  │      Group    Group    Group          Group                      │
  │      (3 replicas)                     (3 replicas)              │
  │                                                                  │
  │   Topic metadata → hashed by topic name → assigned vnode        │
  │   Range metadata → hashed by range ID → assigned vnode          │
  │   Segment lifecycle → tracked by owning range's vnode           │
  └──────────────────────────────────────────────────────────────────┘

  Result:
  → NO single controller
  → 128× more metadata throughput than a single Raft group
  → Localized coordination (a range split affects only its vnode)
  → Vnode failure → only 1/128th of metadata temporarily unavailable
  → Raft ensures consistency within each vnode's shard
```

### DS-RSM Responsibilities

```
  DS-RSM vnode tracks:
  ├── Which topics exist in its keyspace shard
  ├── Which ranges belong to those topics
  ├── Lifecycle of segments in each range:
  │     • ACTIVE  → currently being written to
  │     • SEALED  → immutable, persisted
  │     • DELETED → eligible for GC
  ├── Replica set assignment per segment
  ├── Range split / merge operations
  └── Segment sealing triggers
```

---

## 📡 Failure Detection: The SWIM Protocol

Northguard decouples **metadata management** (DS-RSM/Raft) from **cluster membership** (SWIM gossip). This is a key architectural decision.

```
  TWO SEPARATE CONCERNS:

  1. METADATA CORRECTNESS → DS-RSM / Raft (strong consistency)
     • Raft ensures metadata changes are durable and consistent
     • Writes only proceed when quorum agrees

  2. CLUSTER MEMBERSHIP → SWIM protocol (gossip, eventually consistent)
     • Each broker gossips heartbeats with random peers
     • SWIM = "Scalable Weakly-consistent Infection-style
               Process Group Membership"
     • Fast failure detection: O(log N) rounds to detect failure
     • No single failure-detector bottleneck
     • Gossip spreads infection-style: each node tells others

  WHY THIS MATTERS:
  → Raft quorum is not flooded with heartbeats from 10,000 brokers
  → SWIM handles "is broker X alive?" cheaply
  → Raft handles "which segment replica set is authoritative?" safely
```

---

## 💾 The Storage Layer: fps store (Custom Segment Store)

Northguard uses a custom-built segment storage engine called the **fps store**:

```
  ┌───────────────────────────────────────────────────────────────┐
  │                      fps STORE DESIGN                        │
  │                                                               │
  │  Write Path:                                                  │
  │  ┌──────────┐    ┌─────────┐    ┌────────────┐              │
  │  │ Incoming │───▶│  WAL    │───▶│  Segment   │              │
  │  │ Records  │    │(Write-  │    │  File      │              │
  │  └──────────┘    │Ahead Log│    │(1 GB / 1hr)│              │
  │                  └─────────┘    └────────────┘              │
  │                                                               │
  │  Key design choices:                                          │
  │  • One file per segment (clean GC: delete the file)          │
  │  • Direct I/O (bypass OS page cache)                         │
  │    → Predictable I/O, no page cache eviction surprises       │
  │  • Application-managed cache (not OS page cache)             │
  │    → Cache based on ACTIVE consumer streams, not recency     │
  │  • RocksDB sparse index for fast seeks within a segment       │
  │  • C++ implementation: no GC pauses, deterministic latency   │
  └───────────────────────────────────────────────────────────────┘
```

---

## ⚡ Self-Balancing: How Northguard Eliminates Manual Rebalancing

```
  KAFKA (manual rebalancing):
  
  New broker joins cluster
  → Engineer must run Kafka Reassign Partitions tool
  → Plan JSON partition reassignment
  → Execute (moves terabytes of data)
  → Monitor for hours
  → Cluster disrupted during move

  NORTHGUARD (automatic self-balancing):
  
  New broker joins cluster
  → Broker announces itself via SWIM gossip
  → DS-RSM coordinator identifies it as under-loaded
  → When next segment seals (max 1 hour):
    → New sealed segment assigned to new broker
  → Old broker gradually offloads sealed segments
  → No explicit "rebalance" operation needed
  → No data movement disruption
  → Cluster reaches balance within hours naturally
  
  Similarly for broker failure:
  → SWIM detects broker failure in seconds
  → DS-RSM marks affected segments as under-replicated
  → Segments re-replicated to other brokers in parallel
  → 1 GB segments = fast individual recovery
  → 100 segments recover concurrently → 100 GB recovered fast
```

---

## 🔄 The Northguard Write Path

```
  Producer sends record to Topic T, Key K

  Step 1: Route to Range
  Client → hash(K) → find Range that owns this key → find Range leader

  Step 2: Write to Active Segment
  Range leader → append to Active Segment WAL
              → replicate segment WAL entries to 2 followers
              → ack to producer when quorum acknowledged

  Step 3: Segment Sealing (when 1 GB or 1 hour reached)
  DS-RSM coordinator → seals the active segment
                    → creates new active segment
                    → assigns new segment to leader broker
                    → updates metadata in Raft vnode

  Step 4: Sealed Segment Replication
  Sealed segment → independently replicated to target replica count
                 → moved to optimal storage nodes automatically
```

---

## 🔄 The Northguard Read Path

```
  Consumer fetches from Topic T at offset O

  Step 1: Locate Segment
  Client → query DS-RSM to find which Segment contains offset O
         → DS-RSM returns: Segment ID + Broker location

  Step 2: Fetch from Segment
  Client → connect to broker hosting the segment
         → fetch records starting at offset O
         → broker reads from fps store (cached if active consumer)

  Step 3: Cursor Advance
  Client → next fetch continues on same segment or moves to next
         → DS-RSM provides next segment location if needed
```

---

## 🌟 Key Design Outcomes

```
┌────────────────────────────────────────────────────────────────────┐
│               NORTHGUARD DESIGN OUTCOMES                           │
│                                                                    │
│  ✅  No single controller bottleneck (DS-RSM: 128+ vnodes)        │
│  ✅  Fine-grained recovery (1 GB segments, not TB partitions)      │
│  ✅  Automatic self-balancing (no manual rebalancing)              │
│  ✅  Dynamic scaling (range splits/merges vs fixed partitions)     │
│  ✅  Low latency (C++, Direct I/O, no GC pauses)                  │
│  ✅  Predictable broker failure recovery (seconds to minutes)      │
│  ✅  Lights-out operations (no on-call for hardware changes)       │
│  ✅  Handles 400,000+ topics natively without special tooling      │
└────────────────────────────────────────────────────────────────────┘
```

---

> **Next:** See how LinkedIn migrated all existing Kafka workloads to Northguard without a single line of application code change → [Xinfra Migration Framework](../03-Xinfra-Migration-Framework/README.md)
