import com.ute.recener.util.{DataBaseConnection, Inventory, MappingSchema}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange
import org.apache.hadoop.hbase.filter.{FuzzyRowFilter, MultiRowRangeFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Bytes, Pair}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode, SparkSession, functions}
import org.apache.spark.sql.functions._
import scala.collection.JavaConverters._

import java.util.{ArrayList, Base64, Calendar, Date, LinkedList}
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConverters._
import scala.io.Source
import java.io.FileNotFoundException
import java.time.Duration
import org.apache.spark.sql.types.{IntegerType, StringType}




object IVDaiceExtractor {


  final val cfDataIV = "IV"
  final val cfDataBytes = Bytes.toBytes(cfDataIV)
  private val serialVersionUID = 1L

  private val voltage_phase_1 = "13"
  private val current_phase_1 = "14"
  private val voltage_phase_2 = "15"
  private val current_phase_2 = "16"
  private val voltage_phase_3 = "17"
  private val current_phase_3 = "18"

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "recener")


    val conf = new SparkConf()
      .setAppName("Instantaneous value DAICE Extractor")
    //.set("spark.yarn.jars", "hdfs:///user/deptorecener/fernandaJars/*")
    //.set("spark.submit.deployMode", "client")
    //.set("spark.master", "yarn")
    //.set("spark.driver.host", "172.16.18.11")
    //.set("spark.driver.memory", "100G")
    //.set("spark.driver.port", "9888")
    //.set("spark.dynamicAllocation.enabled", "true")
    //.set("spark.shuffle.service.enabled", "true")
    //.set("spark.dynamicAllocation.minExecutors", "2")
    //.set("spark.dynamicAllocation.maxExecutors", "300")
    //.set("spark.dynamicAllocation.initialExecutors", "10")
    //.set("spark.yarn.queue", "root.datanodo")
    //.set("spark.sql.broadcastTimeout", "-1")
    //.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    //.set("spark.yarn.appMasterEnv.JAVA_HOME", "/usr/java/jdk1.8.0_181-cloudera/jre/")
    //.set("spark.executor.extraClassPath", "/var/cloudera/parcels/CDH-6.1.1-1.cdh6.1.1.p0.875250/lib/spark/jars/*.jar")
    //.set("spark.executorEnv.JAVA_HOME", "/usr/java/jdk1.8.0_181-cloudera/jre/")


    val jdbcUrl = DataBaseConnection.getUrl
    val connectionProperties = DataBaseConnection.getConnectionProperties

    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    @transient val exampleHConf = HBaseConfiguration.create()
    exampleHConf.set("hbase.zookeeper.quorum", "mdmprdmgm.corp.ute.com.uy,mdmprdhed1.corp.ute.com.uy,mdmprdhed2.corp.ute.com.uy")

    val format = new java.text.SimpleDateFormat("yyyy-MM-dd")

    val group: String = args(0)
    val start: Long = args(1).toLong
    val stop: Long = args(2).toLong
    val directory: String = args(3) //example  user/deptorecener/alexOutputs/20230123

    val date1 = "%014d".format(start)
    val date2 = "%014d".format(stop)

    // phase = 0 --> monophasic
    // phase = 1 --> triphasic
    for (phase <- 1 to 1) {

      //val findAllMeasuringPointsSql = Inventory.findMeasuringPointsByGroupAndPhase(group, phase)
      val findAllMeasuringPointsSql = Inventory.findMeasuringPointsBySource(21)
      val measuringPointDf = spark.read.jdbc(jdbcUrl, findAllMeasuringPointsSql, connectionProperties)
      val measuringPointIds: List[String] = measuringPointDf.select("ID").map(_.toString().replace("[", "").replace("]", "")).collect().toList

      // Assuming you have your measuringPointIds as a List[String]
      val batchSize = 10000 // Number of IDs to process in each batch

      // Split the list into batches
      val batches = measuringPointIds.grouped(batchSize).toList

      batches.foreach(batch => {
        // Now 'batch' contains a subset of your measuringPointIds
        // Configure HBase scan for the current batch

        if (batch.length > 0) {

          exampleHConf.set(TableInputFormat.INPUT_TABLE, "MDM_DATA:IV")
          exampleHConf.set(TableInputFormat.SCAN, filterByMultiRowRangeIV(batch, date1, date2))


          val hBaseRDIV: RDD[(ImmutableBytesWritable, Result)] = spark
            .sparkContext
            .newAPIHadoopRDD(
              exampleHConf,
              classOf[TableInputFormat],
              classOf[ImmutableBytesWritable],
              classOf[Result]
            )

          /*Get only magnitudes voltage and electric current*/
          val resultRDDOM = hBaseRDIV.map(tuple => tuple._2)
          val ivRDD = resultRDDOM.map(x => MappingSchema.parseIvRow(x, cfDataIV))


          var magnitudes = Set(voltage_phase_1, current_phase_1)

          if (phase == 1) {
            var tri_magnitudes = Set(voltage_phase_2, current_phase_2, voltage_phase_3, current_phase_3)
            magnitudes = magnitudes ++ tri_magnitudes
          }

          //Filter instantaneous value by magnitude and measurement interval (to keep only instantaneous registers)
          val ivDF = ivRDD.toDF().filter(($"magnitude".isInCollection(magnitudes)) && ($"measurement_interval" === 1))
            .select($"measuring_point", $"date", $"magnitude", $"value")

          //group by measuring_point and get min/max date for each measuring point
          val ivDatesDF = ivDF.groupBy($"measuring_point").agg(
            functions.min(col("date")).as("min_date"),
            functions.max(col("date")).as("max_date"))

          //generate ranges for every measuring point between its min/max date with step = 15 minutes
          val ranges = spark.range(start, stop, Duration.ofMinutes(15).toMillis)
          val dateRangesDF = ranges.select(col("id").alias("date_range"))
          val rangesPerMeasuringPoint = dateRangesDF.crossJoin(ivDatesDF).filter($"min_date" <= $"date_range" && $"date_range" <= $"max_date")
            .withColumnRenamed("measuring_point", "measuring_point_range")

          val ivMagnitudesToColumnDF = ivDF.groupBy($"measuring_point", $"date").pivot($"magnitude").max("value")

          val ivWithNaNDF = rangesPerMeasuringPoint.join(ivMagnitudesToColumnDF, rangesPerMeasuringPoint("measuring_point_range") === ivMagnitudesToColumnDF("measuring_point") && rangesPerMeasuringPoint("date_range") === ivMagnitudesToColumnDF("date"), "left")
            .withColumn("date_range", ($"date_range" / 1000).cast(IntegerType))

          if (phase == 0) { //monophasic
            //dataframe must have this format
            //measuring_point, voltage_phase_1, current_phase_1
            //if there is no value found, complete with NaN

            val ivWithNaNDF2 = ivWithNaNDF.withColumnRenamed("13", "voltage_phase_1")
              .withColumnRenamed("14", "current_phase_1")
              .withColumn("voltage_phase_1", ($"voltage_phase_1").cast(StringType))
              .withColumn("current_phase_1", ($"current_phase_1").cast(StringType))
              .na.fill("NaN", Seq("voltage_phase_1", "current_phase_1"))

            val formattedDF = ivWithNaNDF2.join(measuringPointDf, ivWithNaNDF("measuring_point_range") === measuringPointDf("ID"))
              .select($"CODE", $"date_range", $"voltage_phase_1", $"current_phase_1")
              .withColumnRenamed("CODE", "measuring_point")
              .withColumnRenamed("date_range", "date")
              .orderBy($"measuring_point", $"date")

            formattedDF.repartition(1)
              .write.mode(SaveMode.Append)
              .options(Map("header" -> "true", "delimiter" -> ","))
              .csv("hdfs:////" + directory + "/monophasic")

          }
          else { //triphasic
            //dataframe must have this format
            //measuring_point, voltage_phase_1, voltage_phase_2, voltage_phase_3, current_phase_1, current_phase_2, current_phase_3
            //if there is no value found, complete with NaN

            val ivWithNaNDF2 = ivWithNaNDF.withColumnRenamed("13", "voltage_phase_1")
              .withColumnRenamed("15", "voltage_phase_2")
              .withColumnRenamed("17", "voltage_phase_3")
              .withColumnRenamed("14", "current_phase_1")
              .withColumnRenamed("16", "current_phase_2")
              .withColumnRenamed("18", "current_phase_3")
              .withColumn("voltage_phase_1", ($"voltage_phase_1").cast(StringType))
              .withColumn("voltage_phase_2", ($"voltage_phase_2").cast(StringType))
              .withColumn("voltage_phase_3", ($"voltage_phase_3").cast(StringType))
              .withColumn("current_phase_1", ($"current_phase_1").cast(StringType))
              .withColumn("current_phase_2", ($"current_phase_2").cast(StringType))
              .withColumn("current_phase_3", ($"current_phase_3").cast(StringType))
              .na.fill("NaN", Seq("voltage_phase_1", "voltage_phase_2", "voltage_phase_3", "current_phase_1", "current_phase_2", "current_phase_3"))

            val formattedDF = ivWithNaNDF2.join(measuringPointDf, ivWithNaNDF("measuring_point_range") === measuringPointDf("ID"))
              .select($"CODE", $"date_range", $"voltage_phase_1", $"voltage_phase_2", $"voltage_phase_3", $"current_phase_1", $"current_phase_2", $"current_phase_3")
              .withColumnRenamed("CODE", "measuring_point")
              .withColumnRenamed("date_range", "date")
              .orderBy($"measuring_point", $"date")

            formattedDF.repartition(1)
              .write.mode(SaveMode.Append)
              .options(Map("header" -> "true", "delimiter" -> ","))
              .csv("hdfs:////" + directory + "/triphasic")

//            formattedDF.coalesce(1).
//              write.mode(SaveMode.Overwrite).
//              options(Map("header" -> "true", "delimiter" -> ",")).
//              csv("hdfs:////" + directory + "/triphasic")

          }
        }
      })

    }

    def convertScanToString(scan: Scan): String = {
      val proto = ProtobufUtil.toScan(scan)
      Base64.getEncoder.encodeToString(proto.toByteArray)
    }

    def filterByMultiRowRangeIV(list: List[String], startDate: String, stopDate: String): String = {
      val scan = new Scan()
      val ranges = new ArrayList[RowRange]()

      list.foreach(pm => {
        val startRowKey = pm.reverse.padTo(8, '0').reverse + startDate + "00000"
        val endRowKey = pm.reverse.padTo(8, '0').reverse + stopDate + "99999"
        ranges.add(new RowRange(Bytes.toBytes(startRowKey), true, Bytes.toBytes(endRowKey), true))
      })

      val filter = new MultiRowRangeFilter(ranges)
      scan.setFilter(filter)
      //scan
      convertScanToString(scan)
    }

  }
}