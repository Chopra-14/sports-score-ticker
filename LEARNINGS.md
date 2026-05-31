# LEARNINGS.md

# Kafka Project Learnings

This document summarizes the key concepts learned while building the Sports Score Ticker system using Apache Kafka, FastAPI, Docker, and Kafka CLI tools.

---

# 1. Serialization Analysis

Serialization is the process of converting an object into a format that can be transmitted over a network or stored efficiently.

In this project, events were serialized using JSON before being published to Kafka topics.

Example Event:

```json
{
  "eventId": "123",
  "matchId": "match-1",
  "eventType": "GOAL",
  "team": "India",
  "timestamp": "2026-05-31T10:00:00"
}
```

---

## JSON Serialization

JSON is a text-based serialization format.

Advantages:

* Human readable
* Easy debugging
* Language independent
* Simple implementation

Disadvantages:

* Larger payload size
* Repeated field names
* Higher network overhead

Estimated Size:

```text
JSON Event ≈ 210 Bytes
```

---

## Avro Serialization

Avro is a compact binary serialization format developed within the Apache ecosystem.

Advantages:

* Smaller message size
* Faster transmission
* Schema support
* Better compatibility management

Disadvantages:

* Requires schema management
* Less human readable

Estimated Size:

```text
Avro Event ≈ 120 Bytes
```

---

## Size Comparison

| Format | Approx Size |
| ------ | ----------- |
| JSON   | 210 Bytes   |
| Avro   | 120 Bytes   |

Reduction:

```text
(210 - 120) / 210 × 100

≈ 42.8% smaller
```

---

## Observation

For small projects, JSON is sufficient and easy to maintain.

For large-scale event streaming systems with millions of events per day, Avro provides substantial savings in:

* Storage
* Network bandwidth
* Processing cost

---

# 2. Producer Acknowledgement (acks) Experiment

Kafka producers support different acknowledgement levels.

These settings determine when Kafka confirms message delivery.

---

## acks = 0

Configuration:

```python
acks=0
```

Behavior:

Producer does not wait for any acknowledgement from Kafka.

Advantages:

* Fastest throughput
* Lowest latency

Disadvantages:

* Possible message loss
* No delivery guarantee

Observation:

Messages were sent very quickly, but reliability was poor.

---

## acks = 1

Configuration:

```python
acks=1
```

Behavior:

Producer waits for acknowledgement from the leader broker.

Advantages:

* Good balance between speed and reliability
* Common production setting

Disadvantages:

* Possible message loss if leader fails before replication

Observation:

Slightly slower than acks=0 but significantly safer.

---

## acks = all

Configuration:

```python
acks="all"
```

Behavior:

Producer waits until all in-sync replicas acknowledge the message.

Advantages:

* Highest durability
* Strongest delivery guarantees

Disadvantages:

* Highest latency
* Slight throughput reduction

Observation:

Most reliable configuration.

Messages were consistently persisted before acknowledgement.

---

## Acknowledgement Comparison

| Setting  | Speed     | Reliability |
| -------- | --------- | ----------- |
| acks=0   | Very High | Low         |
| acks=1   | High      | Medium      |
| acks=all | Moderate  | Very High   |

---

## Conclusion

For critical event processing systems:

```text
acks=all
```

is the safest configuration because it minimizes the possibility of message loss.

For this project, reliability was considered more important than raw throughput.

---

# 3. Consumer Scaling Experiment

Kafka enables horizontal scaling through consumer groups.

The match-events topic was configured with:

```text
Partitions = 5
```

This allows Kafka to distribute partitions across multiple consumers.

---

## Scenario 1: One Consumer

Configuration:

```text
Consumer Instances = 1
Partitions = 5
```

Assignment:

```text
Consumer-1

P0
P1
P2
P3
P4
```

Observation:

Single consumer handled all partitions.

No parallel processing occurred.

---

## Scenario 2: Two Consumers

Configuration:

```text
Consumer Instances = 2
Partitions = 5
```

Assignment:

```text
Consumer-1

P0
P1
P2

Consumer-2

P3
P4
```

Observation:

Kafka automatically redistributed partitions.

Load was shared between consumers.

Processing throughput increased.

---

## Scenario 3: Three Consumers

Configuration:

```text
Consumer Instances = 3
Partitions = 5
```

Assignment:

```text
Consumer-1

P0
P1

Consumer-2

P2
P3

Consumer-3

P4
```

Observation:

All consumers became active.

Kafka balanced work across the group.

---

# Kafka Rebalancing

Whenever a consumer joins or leaves a consumer group, Kafka performs a rebalance.

Example:

```text
Consumer-3 joins
```

Kafka automatically redistributes partitions.

Example:

Before:

Consumer-1 → P0 P1 P2 P3 P4

After:

Consumer-1 → P0 P1

Consumer-2 → P2 P3

Consumer-3 → P4

---

## Benefits of Rebalancing

* Fault tolerance
* Automatic workload distribution
* Horizontal scalability
* High availability

---

# Key Learnings

Throughout this project I learned:

1. Kafka topics are partitioned for scalability.
2. Consumer groups enable parallel processing.
3. Rebalancing automatically redistributes partitions.
4. JSON serialization is easy to use but larger in size.
5. Avro significantly reduces payload size.
6. Producer acknowledgements directly affect reliability.
7. acks=all provides the strongest durability guarantees.
8. Docker Compose simplifies deployment and reproducibility.
9. Kafka CLI tools are valuable for monitoring and debugging.
10. End-to-end event streaming systems require careful coordination between producers, consumers, and topic configuration.

---

# Final Conclusion

This project provided practical experience with real-world event-driven architecture using Apache Kafka. The implementation demonstrated producer-consumer communication, topic partitioning, consumer groups, lag monitoring, Docker deployment, and API integration. The experiments conducted on serialization, acknowledgements, and consumer scaling highlighted the trade-offs between performance, reliability, and scalability in distributed systems.
