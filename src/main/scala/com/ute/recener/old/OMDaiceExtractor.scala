package com.ute.recener.old

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
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.{SaveMode, SparkSession, functions}

import java.time.Duration
import java.util.{ArrayList, Base64, Calendar, Date, LinkedList}
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConverters._


object OMDaiceExtractor {

  def formatDateTime(date: String, period: Int): String = {

    var h = period / 4
    val m = (period % 4) * 15
    if (h == 24) {
      h = 0
    }

    val d = date.substring(0, 10)
    val hour = "%02d".format(h)
    val minute = "%02d".format(m)

    d + " " + hour + ":" + minute

  }
  val calculateDatetime = udf { (d: String, p: Int) => formatDateTime(d, p) }

  final val cfDataOM = "OM"
  final val cfDataBytes = Bytes.toBytes(cfDataOM)
  private val serialVersionUID = 1L

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "recener")


    val conf = new SparkConf()
      .setAppName("Optimal Measure DAICE Extractor")
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

    val d1 = start / 3600000
    val date1 = "%010d".format(d1)
    val d2 = stop / 3600000
    val date2 = "%010d".format(d2)

    val mi = "1"

    // Parameters for fuzzy filter
    val startDate = Calendar.getInstance()
    startDate.setTime(new Date(start))
    val endDate = Calendar.getInstance()
    endDate.setTime(new Date(stop))
    val ae = "1"
    val q1 = "3"
    val mm : Set[String] = Set(ae, q1)
    // End Parameters for fuzzy filter


    val findAllMeasuringPointsSql = Inventory.findMeasuringPointsByGroup(group)
    val measuringPointDf = spark.read.jdbc(jdbcUrl, findAllMeasuringPointsSql, connectionProperties)
    val measuringPointIds: List[String] = measuringPointDf.select("ID").map(_.toString().replace("[", "").replace("]", "")).collect().toList

    if (measuringPointIds.length > 0) {

      exampleHConf.set(TableInputFormat.INPUT_TABLE, "MDM_DATA:OM")
      //exampleHConf.set(TableInputFormat.SCAN, filterByMultiRowRangeOM(measuringPointIds, date1, date2, mi))
      val fuzzyRows: Seq[FuzzyData] = fuzzyFilterList(startDate, endDate, mm, mi)
      exampleHConf.set(TableInputFormat.SCAN, filterByFuzzy(fuzzyRows))


      val hBaseRDOM: RDD[(ImmutableBytesWritable, Result)] = spark
        .sparkContext
        .newAPIHadoopRDD(
          exampleHConf,
          classOf[TableInputFormat],
          classOf[ImmutableBytesWritable],
          classOf[Result]
        )

      /*Get only magnitudes voltage and electric current*/
      val resultRDDOM = hBaseRDOM.map(tuple => tuple._2)
      val omRDD = resultRDDOM.map(x => MappingSchema.parseOmRow(x, cfDataOM))

      /*
        1	active_energy
        3	reactive_energy_1
      */

      var magnitudes = Set("1", "3")

      //Filter measure by magnitude and source
      val omDF = omRDD.toDF().filter($"source" === 41) //$"magnitude".isInCollection(magnitudes) this filter is needed only when multirowrange is used
        .withColumn("day", $"day" + $"period" * 15 * 60 * 1000)
        .select($"measuring_point", $"day", $"magnitude", $"value")

      //println(omDF.count())

      //group by measuring_point and get min/max date for each measuring point
      val omDatesDF = omDF.groupBy($"measuring_point").agg(
        functions.min(col("day")).as("min_date"),
        functions.max(col("day")).as("max_date"))

      //generate ranges for every measuring point between its min/max date with step = 15 minutes
      val ranges = spark.range(start, stop, Duration.ofMinutes(15).toMillis)
      val dateRangesDF = ranges.select(col("id").alias("day_range"))
      val rangesPerMeasuringPoint = dateRangesDF.crossJoin(omDatesDF).filter($"min_date" <= $"day_range" && $"day_range" <= $"max_date")
        .withColumnRenamed("measuring_point", "measuring_point_range")

      val omMagnitudesToColumnDF = omDF.groupBy($"measuring_point", $"day").pivot($"magnitude").max("value")

      val omWithNaNDF = rangesPerMeasuringPoint.join(omMagnitudesToColumnDF, rangesPerMeasuringPoint("measuring_point_range") === omMagnitudesToColumnDF("measuring_point") && rangesPerMeasuringPoint("day_range") === omMagnitudesToColumnDF("day"), "left")
        .withColumn("day_range", ($"day_range" / 1000).cast(IntegerType))

      //dataframe must have this format
      //measuring_point, active_energy, reactive_energy_1
      //if there is no value found, complete with NaN

      //println(omWithNaNDF.count())

      val omWithNaNDF2 = omWithNaNDF.withColumnRenamed("1", "active_energy")
        .withColumnRenamed("3", "reactive_energy_1")
        .withColumn("active_energy", ($"active_energy").cast(StringType))
        .withColumn("reactive_energy_1", ($"reactive_energy_1").cast(StringType))
        .na.fill("NaN", Seq("active_energy", "reactive_energy_1"))

      val formattedDF = omWithNaNDF2.join(measuringPointDf, omWithNaNDF("measuring_point_range") === measuringPointDf("ID"))
        .select($"CODE", $"day_range", $"active_energy", $"reactive_energy_1")
        .withColumnRenamed("CODE", "measuring_point")
        .withColumnRenamed("day_range", "day")
        .orderBy($"measuring_point", $"day")

      formattedDF.coalesce(1).
        write.mode(SaveMode.Overwrite).
        options(Map("header" -> "false", "delimiter" -> ",")).
        csv("hdfs:////user/deptorecener/fernandaOutputs/omDAICE/")
    }
  }

  def convertScanToString(scan: Scan): String = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.getEncoder.encodeToString(proto.toByteArray)
  }

  def filterByMultiRowRangeOM(list: List[String], startDate: String, stopDate: String, mi: String): String = {
    val scan = new Scan()
    val ranges = new ArrayList[RowRange]()

    list.foreach(pm => {
      val startRowKey = pm.reverse.padTo(8, '0').reverse + mi + startDate + "00000"
      val endRowKey = pm.reverse.padTo(8, '0').reverse + mi + stopDate + "99999"
      ranges.add(new RowRange(Bytes.toBytes(startRowKey), true, Bytes.toBytes(endRowKey), true))
    })

    val filter = new MultiRowRangeFilter(ranges)
    scan.setFilter(filter)
    //scan
    convertScanToString(scan)
  }

  def fuzzyFilterList(start: Calendar, end: Calendar, magnitudes: Set[String], mi: String): Seq[FuzzyData] = {

    val listFuzzy = new LinkedList[FuzzyData]()

    while (end.after(start) || end.equals(start)) {
      val d = start.getTime.getTime / 3600000
      val date = "%010d".format(d)
      for( m <- magnitudes){
        val fuzzyData: FuzzyData = FuzzyData(
          rowKeyPattern = "????????" + mi + date + m,
          maskInfo =
            "\\x01\\x01\\x01\\x01\\x01\\x01\\x01\\x01\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00"
        )
        listFuzzy.add(fuzzyData)
      }
      start.add(Calendar.DAY_OF_MONTH, 1)
    }
    listFuzzy.toSeq
  }

  case class FuzzyData(rowKeyPattern: String, maskInfo: String)

  def filterByFuzzy(fuzzyRows: Seq[FuzzyData]) = {
    val fuzzyRowsPair = {
      fuzzyRows map { fuzzyData =>
        new Pair(
          Bytes.toBytesBinary(fuzzyData.rowKeyPattern),
          Bytes.toBytesBinary(fuzzyData.maskInfo)
        )
      }
    }.asJava

    val fuzzyFilter = new FuzzyRowFilter(fuzzyRowsPair)
    val scan = new Scan()
    //val f  = new SingleColumnValueFilter()
    scan.setFilter(fuzzyFilter)
    //TableMapReduceUtil.convertStringToScan(scan)
    convertScanToString(scan)
  }

}