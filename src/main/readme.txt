  126  20231009_114530 ls
  127  20231009_114533 pwd
  128  20231009_114540 cd /user
  129  20231009_114546 cd ~/user
  130  20231009_114549 cd ~
  131  20231009_114550 ls
  132  20231009_114553 pwd
  133  20231009_114555 cd ..
  134  20231009_114610 pwd
  135  20231009_114621 ls
  136  20231009_114629 cd home
  137  20231009_114630 ls
  138  20231009_114642 hbase shell
  139  20231009_115120 hadoop fs
  140  20231009_115131 hadoop fs -ls
  141  20231009_115220 hadoop fs -ls /user/deptorecener/alexOutputs
  142  20231009_115257 hadoop fs -ls /user/deptorecener/alexOutputs/202310072120/omDAICE/rangesPerMeasuringPoint
  198  20231109_112152 spark-submit --class OMDaiceExtractorV3 --master yarn --deploy-mode cluster --conf spark.kryoserializer.buffer.max=1024 --conf spark.sql.broadcastTimeout=-1 --conf spark.dynamicAllocation.initialExecutors=5 --conf spark.dynamicAllocation.minExecutors=5 --conf spark.dynamicAllocation.maxExecutors=300 --conf spark.driver.memory=20gb --executor-memory 5G --jars $(echo ../../jars/*.jar | tr ' ' ',') --queue root.datanodo DAICE-spark-1.0-SNAPSHOT-jar-with-dependencies.jar 202306300000 202310020000
  209  20231129_154805 spark-submit --class IndirectMeasureExtractorOptimized --master yarn --deploy-mode cluster --conf spark.kryoserializer.buffer.max=1024 --conf spark.sql.broadcastTimeout=-1 --conf spark.dynamicAllocation.initialExecutors=5 --conf spark.dynamicAllocation.minExecutors=5 --conf spark.dynamicAllocation.maxExecutors=300 --conf spark.driver.memory=20g --executor-memory 8GB --conf spark.memory.offHeap.enabled=true --conf spark.memory.offHeap.size=4g --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.memory.fraction=0.3 --jars $(echo ../../jars/*.jar | tr ' ' ',') --queue root.datanodo DAICE-spark-1.0-SNAPSHOT-jar-with-dependencies.jar 202310290000 202310290200 user/deptorecener/indirectMeasures/20231121
