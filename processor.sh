#!/bin/bash

#=================================================
# run processors jar
#=================================================

echo "Running netflix processor"


CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)

java -cp /usr/lib/kafka/libs/*:NetflixProcessor.jar \
 com.example.bigdata.NetflixPrizeProcessing ${CLUSTER_NAME}-w-0:9092 movie_titles.csv


echo "Done"
