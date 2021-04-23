#!/bin/bash

echo "Deleting all kafka topics"

CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)

TOPICS=$(kafka-topics.sh --zookeeper localhost:2181 --list)

for T in $TOPICS
do
  if [ "$T" != "__consumer_offsets" ]; then
    kafka-topics.sh --zookeeper localhost:2181 --delete --topic $T
  fi
done

echo "Topics deleted"


