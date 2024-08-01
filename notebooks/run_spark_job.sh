#!/bin/bash

# Base spark-submit command
BASE_COMMAND="spark-submit --class OMInspectionsExtractor --master yarn --deploy-mode cluster \
  --conf spark.executor.memory=20G --conf spark.executor.memoryOverhead=2G \
  --conf spark.driver.memoryOverhead=2048 --conf spark.driver.memory=40G \
  --conf spark.kryoserializer.buffer.max=1024m --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.sql.broadcastTimeout=-1 --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.initialExecutors=50 --conf spark.dynamicAllocation.minExecutors=50 \
  --conf spark.dynamicAllocation.maxExecutors=800 --conf spark.shuffle.service.enabled=true \
  --conf spark.default.parallelism=100 --conf spark.sql.shuffle.partitions=100 \
  --conf spark.rpc.message.maxSize=800 --conf spark.memory.offHeap.enabled=true \
  --conf spark.memory.offHeap.size=4g --jars $(echo ../../jars/*.jar | tr ' ' ',') \
  --queue root.datanodo DAICE-spark-1.0-SNAPSHOT-jar-with-dependencies.jar 1"

# Loop to run the job with different second parameter values
for i in {1..9}
do
  echo "Running Spark job with parameter $i"
  $BASE_COMMAND $i
done
