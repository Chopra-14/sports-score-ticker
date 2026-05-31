import json
import random
import time
import uuid

from datetime import datetime

from kafka import KafkaProducer

import os

KAFKA_SERVER = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

producer = None

while producer is None:
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

        print("Connected to Kafka")

    except Exception:
        print("Waiting for Kafka...")
        time.sleep(5)

matches = {
    "match-1": ["India", "Australia"],
    "match-2": ["England", "Pakistan"],
    "match-3": ["South Africa", "New Zealand"],
    "match-4": ["Sri Lanka", "Bangladesh"],
    "match-5": ["West Indies", "Afghanistan"]
}


event_types = [
    "GOAL",
    "FOUL",
    "CORNER",
    "YELLOW_CARD",
    "RED_CARD"
]


print(f"Producer connected to Kafka at {KAFKA_SERVER}")
print("Producer started...")


while True:

    match_id = random.choice(list(matches.keys()))

    teams = matches[match_id]

    event_type = random.choice(event_types)

    team = random.choice(teams)

    event = {
        "eventId": str(uuid.uuid4()),
        "matchId": match_id,
        "eventType": event_type,
        "team": team,
        "timestamp": datetime.utcnow().isoformat()
    }

    producer.send(
        "match-events",
        value=event
    )

    if event_type == "GOAL":
        producer.send(
            "match-alerts",
            value=event
        )

    producer.flush()

    print(
        f"[{event['eventType']}] "
        f"{event['matchId']} | "
        f"{event['team']}"
    )

    time.sleep(2)