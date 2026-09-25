#!/usr/bin/env bash

RANDOM_KAFKA_CLUSTER_UUID=$(kafka-storage.sh random-uuid)

mkdir -p /tmp/tds

if [[ ! -f /tmp/tds/consumer_group_id ]]; then
    kafka-storage.sh random-uuid > /tmp/tds/consumer_group_id
fi

kafka-storage.sh format \
    --standalone \
    -t "$RANDOM_KAFKA_CLUSTER_UUID" \
    -c "$KAFKA_CONFIG/server.properties"

kafka-server-start.sh \
    "$KAFKA_CONFIG/server.properties" &

