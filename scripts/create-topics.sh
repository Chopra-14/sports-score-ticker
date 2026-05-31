#!/bin/bash

sleep 20

kafka-topics \
--create \
--if-not-exists \
--topic match-events \
--bootstrap-server kafka:29092 \
--partitions 5 \
--replication-factor 1

kafka-topics \
--create \
--if-not-exists \
--topic score-updates \
--bootstrap-server kafka:29092 \
--partitions 5 \
--replication-factor 1

kafka-topics \
--create \
--if-not-exists \
--topic match-alerts \
--bootstrap-server kafka:29092 \
--partitions 1 \
--replication-factor 1