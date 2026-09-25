#!/usr/bin/env bash

kafka-topics.sh \
    --create \
    --topic camera-topic \
    --partitions 3 \
    --bootstrap-server localhost:9092
