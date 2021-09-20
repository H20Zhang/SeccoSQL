#!/usr/bin/env bash
$SPARK_HOME/bin/spark-submit \
--master yarn \
--deploy-mode client \
--driver-memory 16g \
--driver-cores 4 \
--num-executors 12 \
--executor-memory 28g \
--executor-cores 7 \
--conf spark.memory.fraction=0.4 \
--conf spark.memory.storageFraction=0.5 \
--conf spark.network.timeout=10000000 \
--conf spark.speculation=false \
--conf spark.locality.wait=0s \
--conf spark.shuffle.io.numConnectionsPerPeer=3 \
$@