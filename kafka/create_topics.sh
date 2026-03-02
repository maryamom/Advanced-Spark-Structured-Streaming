#!/bin/bash
# Create topics for Spark lab. Run from host: docker exec -it kafka bash -c "kafka-topics --create --topic events --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092"
kafka-topics --create --topic events --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
kafka-topics --create --topic events-valid --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
kafka-topics --create --topic events-invalid --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092
