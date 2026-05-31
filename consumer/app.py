print("CONSUMER APP LOADED")
import json
import os
import threading
from collections import deque

from fastapi import FastAPI
from kafka.admin import KafkaAdminClient
from kafka import KafkaConsumer, KafkaAdminClient, TopicPartition
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

scores = {
    "match-1": {
        "teams": ["India", "Australia"],
        "score": [0, 0],
        "status": "LIVE"
    },
    "match-2": {
        "teams": ["England", "Pakistan"],
        "score": [0, 0],
        "status": "LIVE"
    },
    "match-3": {
        "teams": ["South Africa", "New Zealand"],
        "score": [0, 0],
        "status": "LIVE"
    },
    "match-4": {
        "teams": ["Sri Lanka", "Bangladesh"],
        "score": [0, 0],
        "status": "LIVE"
    },
    "match-5": {
        "teams": ["West Indies", "Afghanistan"],
        "score": [0, 0],
        "status": "LIVE"
    }
}


alerts = deque(maxlen=10)


KAFKA_SERVER = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

def consume_match_events():

    consumer = KafkaConsumer(
        "match-events",
        bootstrap_servers=KAFKA_SERVER,
        value_deserializer=lambda m: json.loads(
            m.decode("utf-8")
        ),
        auto_offset_reset="earliest",
        group_id="sports-consumer-group"
    )

    print("Listening to match-events...")

    for message in consumer:

        event = message.value

        match_id = event["matchId"]

        event_type = event["eventType"]

        team = event["team"]

        if event_type == "GOAL":

            teams = scores[match_id]["teams"]

            if team == teams[0]:
                scores[match_id]["score"][0] += 1
            else:
                scores[match_id]["score"][1] += 1

        print("EVENT:", event)
        
def consume_alerts():

    consumer = KafkaConsumer(
        "match-alerts",
        bootstrap_servers=KAFKA_SERVER,
        value_deserializer=lambda m: json.loads(
            m.decode("utf-8")
        ),
        auto_offset_reset="earliest",
        group_id="alerts-consumer-group"
    )
    alerts.appendleft(event)
    print("ALERT:", event)
    print("Listening to match-alerts...")

    for message in consumer:

        event = message.value

        if event["eventType"] == "GOAL":
            alerts.appendleft(event)

        print("ALERT:", event)
def start_consumers():

    event_thread = threading.Thread(
        target=consume_match_events,
        daemon=True
    )

    alert_thread = threading.Thread(
        target=consume_alerts,
        daemon=True
    )

    event_thread.start()
    alert_thread.start()
    


start_consumers()
@app.get("/")
def home():

    return {
        "message": "Consumer running"
    }
@app.get("/scores")
def get_scores():

    return {
        match_id: data
        for match_id, data in scores.items()
    }
@app.get("/alerts")
def get_alerts():

    return list(alerts)
@app.get("/lag")
def get_lag():

    topics = [
        "match-events",
        "match-alerts"
    ]

    lag_info = {}

    try:

        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_SERVER,
            group_id="sports-consumer-group"
        )

        for topic in topics:

            partitions = consumer.partitions_for_topic(topic)

            if not partitions:
                lag_info[topic] = {}
                continue

            topic_lag = {}

            for partition in partitions:

                tp = TopicPartition(
                    topic,
                    partition
                )

                consumer.assign([tp])

                consumer.seek_to_end(tp)

                latest_offset = consumer.position(tp)

                consumer.seek_to_beginning(tp)

                current_offset = consumer.position(tp)

                lag = latest_offset - current_offset

                topic_lag[str(partition)] = lag

            lag_info[topic] = topic_lag

        consumer.close()

        return lag_info

    except Exception as e:

        return {
            "error": str(e)
        }