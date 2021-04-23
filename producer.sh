#!/bin/bash

#=================================================
# run producer jar
#=================================================
echo "Running netflix producer"


CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)

CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)
java -cp /usr/lib/kafka/libs/*:KafkaProducer.jar \
 com.example.bigdata.TestProducer netflix_data 15 kafka-producer-topic \
 0 ${CLUSTER_NAME}-w-0:9092


echo "Done"
