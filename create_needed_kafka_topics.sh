#!/bin/bash

#=================================================
# make needed topics
#=================================================
echo "Making needed kafka topics"


CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)

kafka-topics.sh --zookeeper localhost:2181 --create \
 --replication-factor 1 --partitions 1 --topic kafka-producer-topic

kafka-topics.sh --zookeeper localhost:2181 --create \
 --replication-factor 1 --partitions 1 --topic kafka-consumer-topic
 
kafka-topics.sh --zookeeper localhost:2181 --create \
 --replication-factor 1 --partitions 1 --topic kafka-anomalies-topic
 
 
echo "Done"
