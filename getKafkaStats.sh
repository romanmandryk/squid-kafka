#!/bin/bash

# Set the topic name. Use 'all' for all topics or specify a topic name
TOPIC=${1:-all}

# Function to get message count for a specific topic
get_topic_count() {
    local topic=$1
    docker exec hello-squid-kafka-1 /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
        --broker-list localhost:9092 \
        --topic "$topic" \
        --time -1 \
        --offsets 1 \
        | awk -F ":" '{sum += $3} END {print sum}'
}

# Main script
if [ "$TOPIC" = "all" ]; then
    echo "Fetching message counts for all topics:"
    topics=$(docker exec hello-squid-kafka-1 /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092)
    for topic in $topics; do
        count=$(get_topic_count "$topic")
        echo "Topic: $topic, Messages: $count"
    done
else
    count=$(get_topic_count "$TOPIC")
    echo "Topic: $TOPIC, Messages: $count"
fi