# README.md

# 🏏 Sports Score Ticker using Apache Kafka

A real-time event-driven sports score tracking system built using Apache Kafka, FastAPI, Docker, and a lightweight frontend dashboard.

The system simulates live sports events, streams them through Kafka topics, processes them using Kafka consumers, and exposes APIs for live scores, alerts, and consumer lag monitoring.

---

# Project Overview

This project demonstrates a complete event streaming architecture using Apache Kafka.

Features:

* Real-time sports event generation
* Kafka-based event streaming
* Topic partitioning and consumer groups
* Score tracking API
* Match alert API
* Consumer lag monitoring API
* Dockerized deployment
* Kafdrop Kafka monitoring UI
* Automatic topic creation

---

# Architecture

## High-Level Architecture Diagram

```text id="2wh8vq"
                    +------------------+
                    |     Producer     |
                    |  Sports Events   |
                    +--------+---------+
                             |
                             v
                   +-------------------+
                   |   match-events    |
                   |   5 Partitions    |
                   +---------+---------+
                             |
                             v
                   +-------------------+
                   |     Consumer      |
                   | FastAPI Service   |
                   +----+---------+----+
                        |         |
                        |         |
                        v         v

               +-------------+   +--------------+
               | score-updates|  | match-alerts |
               | 5 Partitions |  | 1 Partition  |
               +------+------+  +------+-------+
                      |                |
                      +-------+--------+
                              |
                              v

                    +----------------+
                    |  REST APIs     |
                    | /scores        |
                    | /alerts        |
                    | /lag           |
                    +--------+-------+
                             |
                             v

                    +----------------+
                    |   Frontend     |
                    | Dashboard UI   |
                    +----------------+

                             |
                             v

                    +----------------+
                    |    Kafdrop     |
                    | Kafka Monitor  |
                    +----------------+
```

---

# Technology Stack

* Apache Kafka
* Zookeeper
* FastAPI
* Python
* Docker
* Docker Compose
* Kafdrop
* HTML
* CSS
* JavaScript

---

# Project Structure

```text id="xlpzzt"
sports-score-ticker/

├── producer/
│   ├── producer.py
│   ├── requirements.txt
│   └── Dockerfile
│
├── consumer/
│   ├── app.py
│   ├── requirements.txt
│   └── Dockerfile
│
├── frontend/
│   ├── index.html
│   ├── style.css
│   └── script.js
│
├── scripts/
│   └── create-topics.sh
│
├── docs/
│
├── README.md
├── CLI_JOURNAL.md
├── LEARNINGS.md
├── docker-compose.yml
└── .gitignore
```

---

# Setup Instructions

## Prerequisites

* Docker Desktop
* Docker Compose

---

## Start the Entire Project

Run:

```bash id="6vbz0g"
docker compose up --build
```

This command automatically starts:

* Zookeeper
* Kafka
* Topic Initialization Service
* Producer Application
* Consumer Application
* Kafdrop

---

## Verify Running Services

```bash id="0x1mym"
docker ps
```

Expected containers:

```text id="7kk3a9"
zookeeper
kafka
producer-app
consumer-app
kafdrop
```

---

# Access URLs

## Consumer API

```text id="1vt5ww"
http://localhost:8080
```

## Swagger Documentation

```text id="d8mnri"
http://localhost:8080/docs
```

## Kafdrop Dashboard

```text id="n9d5t6"
http://localhost:9000
```

---

# API Endpoints

## GET /scores

Returns live scores for all matches.

Example:

```json id="cfx0mq"
{
  "match-1": {
    "teams": ["India", "Australia"],
    "score": [2, 1],
    "status": "LIVE"
  }
}
```

---

## GET /alerts

Returns latest alert events.

Example:

```json id="mjlwmr"
[
  {
    "eventType": "GOAL",
    "matchId": "match-2"
  }
]
```

---

## GET /lag

Returns partition-wise consumer lag.

Example:

```json id="73ynwq"
{
  "match-events": {
    "0": 0,
    "1": 2
  }
}
```

---

# Kafka Topics

| Topic         | Partitions | Purpose                 |
| ------------- | ---------- | ----------------------- |
| match-events  | 5          | Raw sports events       |
| score-updates | 5          | Processed score updates |
| match-alerts  | 1          | Critical match alerts   |

---

# Topic Design Rationale

## match-events

Partitions:

```text id="tixjsm"
5
```

Reason:

The match-events topic receives the highest volume of traffic.

Using five partitions enables:

* Parallel consumption
* Better throughput
* Horizontal scaling
* Load balancing

This allows multiple consumers to process events concurrently.

---

## score-updates

Partitions:

```text id="d0aqso"
5
```

Reason:

Score updates may also grow significantly as event volume increases.

Five partitions ensure:

* Scalability
* Efficient processing
* Future expansion support

---

## match-alerts

Partitions:

```text id="sqv6o3"
1
```

Reason:

Alert events require strict ordering.

Example:

```text id="l4w2cb"
GOAL
GOAL
RED_CARD
```

must appear exactly in the order generated.

Using a single partition guarantees message ordering.

---

# Consumer Groups

Consumer groups were used to:

* Enable fault tolerance
* Support scaling
* Distribute partitions automatically
* Reduce processing bottlenecks

Kafka automatically rebalances partitions whenever consumers join or leave a group.

---

# Monitoring

Kafdrop was integrated to provide:

* Topic inspection
* Consumer group monitoring
* Partition visibility
* Message browsing

Access:

```text id="x96wlf"
http://localhost:9000
```

---

# Key Learnings

During development the following Kafka concepts were explored:

* Serialization (JSON vs Avro)
* Producer acknowledgements
* Consumer groups
* Consumer lag
* Topic partitioning
* Kafka rebalancing
* Dockerized deployment

Detailed findings are documented in:

```text id="swjlwm"
LEARNINGS.md
```

---

# Author

Konakalla Chopra Lakshmi Sathvika

Built as part of the Kafka Event Streaming Project to demonstrate real-time producer-consumer communication using Apache Kafka and Docker.
