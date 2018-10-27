#!/bin/sh
cd /Applications/kafka_2.11-2.0.0/bin

# Run with "localhost" so Kafka is available when laptop has no network
./kafka-server-start.sh ../config/server.properties \
	--override advertised.host.name=localhost \
	--override broker.id=2 \
	--override log.dirs=/tmp/kafka-logs-2 \
	--override port=9094
