package com.ute.recener.old

import com.ute.recener.util.MappingSchema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.filter.FuzzyRowFilter
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Bytes, Pair}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}

import java.text.SimpleDateFormat
import java.time.Duration
import java.util.{Base64, Calendar, Date, TimeZone}
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object OMDaiceExtractorV3 {

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

  final val cfDataOM = "OM"

  def main(args: Array[String]): Unit = {
    // Set Hadoop properties
    System.setProperty("HADOOP_USER_NAME", "recener")

    // Initialize Spark
    val conf = new SparkConf().setAppName("Optimal Measure DAICE Extractor")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    // Initialize a logger (make sure to import org.apache.log4j.Logger)
    val logger = Logger.getLogger("DAICE OM")

    // Configure HBase
    val exampleHConf = HBaseConfiguration.create()
    exampleHConf.set("hbase.zookeeper.quorum", "mdmprdmgm.corp.ute.com.uy,mdmprdhed1.corp.ute.com.uy,mdmprdhed2.corp.ute.com.uy")

    // Parse arguments
    val dateFormat = new SimpleDateFormat("yyyyMMddHHmm")
    dateFormat.setTimeZone(TimeZone.getTimeZone("GMT-3")) // Set Uruguay timezone

    val startString = args(0)
    val stopString = args(1)

    val start = dateFormat.parse(startString).getTime
    val stop = dateFormat.parse(stopString).getTime

    // Log the parsed arguments
    logger.info(s"Parsed Arguments: ")
    logger.info(s"Start String: $startString")
    logger.info(s"Stop String: $stopString")
    logger.info(s"Parsed Start Time: $start")
    logger.info(s"Parsed Stop Time: $stop")

    // Generate date strings
    val measureInterval = "1"

    // Fuzzy filter parameters
    val startDate = Calendar.getInstance()
    startDate.setTime(new Date(start))
    val endDate = Calendar.getInstance()
    endDate.setTime(new Date(stop))
    val activeEnergy = "1"
    val q1 = "3"
    val measureMagnitude: Set[String] = Set(activeEnergy, q1)

    // Configure HBase table and filters
    configureHBaseInput(exampleHConf, "MDM_DATA:OM", fuzzyFilterList(startDate, endDate, measureMagnitude, measureInterval))

    // Read data from HBase into an RDD
    val hBaseRDD = readFromHBase(spark, exampleHConf)

    // Parse the HBase rows into a more manageable format
    val parsedRDD = hBaseRDD.map(x => MappingSchema.parseOmRow(x, cfDataOM))

    // Define the magnitudes to filter ("1" and "3")
    val magnitudes = Set("1", "3")

    // Transform original data to a DataFrame and filter it
    val omDF = transformOmData(spark, parsedRDD.toDF(), 41, magnitudes)

    // Calculate the min and max date ranges for each measuring point
    val omDatesDF = calculateDateRanges(spark, omDF)

    // Generate time ranges based on min/max dates for each measuring point
    val rangesPerMeasuringPoint = generateTimeRanges(spark, start, stop, omDatesDF)

    // Aggregate data by measuring point and day, pivoting the magnitudes
    val omAggregatedDF = aggregateOmData(spark, omDF)

    // Format the output DataFrame
    val finalDF = formatOutputData(spark, rangesPerMeasuringPoint, omAggregatedDF)

    // Save the final DataFrame to HDFS
    saveToHDFS(finalDF, "finalDF")
  }

  case class FuzzyData(rowKeyPattern: String, maskInfo: String)

  private object Constants {
    val MaskInfo = "\\x01\\x01\\x01\\x01\\x01\\x01\\x01\\x01\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00"
    val TimeDivider = 3600000
  }

  def fuzzyFilterList(start: Calendar, end: Calendar, magnitudes: Set[String], measurementInfo: String): Seq[FuzzyData] = {
    import Constants._

    val listFuzzy = ListBuffer[FuzzyData]()
    val tempStart = start.clone().asInstanceOf[Calendar]

    while (end.after(tempStart) || end.equals(tempStart)) {
      val d = tempStart.getTime.getTime / TimeDivider
      val date = "%010d".format(d)

      for {
        m <- magnitudes
      } {
        val fuzzyData = FuzzyData(
          rowKeyPattern = "????????" + measurementInfo + date + m,
          maskInfo = MaskInfo
        )
        listFuzzy += fuzzyData
      }
      tempStart.add(Calendar.DAY_OF_MONTH, 1)
    }
    listFuzzy.toSeq
  }

  def filterByFuzzy(fuzzyRows: Seq[FuzzyData]): String = {
    val fuzzyRowsPair = fuzzyRows.map { fuzzyData =>
      new Pair(
        Bytes.toBytesBinary(fuzzyData.rowKeyPattern),
        Bytes.toBytesBinary(fuzzyData.maskInfo)
      )
    }.asJava

    val fuzzyFilter = new FuzzyRowFilter(fuzzyRowsPair)
    val scan = new Scan()
    scan.setFilter(fuzzyFilter)

    convertScanToString(scan)
  }

  private def configureHBaseInput(conf: Configuration, tableName: String, filters: Seq[FuzzyData]): Unit = {
    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    conf.set(TableInputFormat.SCAN, filterByFuzzy(filters))
  }

  private def readFromHBase(spark: SparkSession, conf: Configuration): RDD[Result] = {
    spark.sparkContext.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    ).map(_._2)
  }

  private def transformOmData(spark: SparkSession, df: DataFrame, source: Int, magnitudes: Set[String]): DataFrame = {
    import spark.implicits._
    df.filter($"source" === source)
      .withColumn("day", $"day" + $"period" * 15 * 60 * 1000)
      .select($"measuring_point", $"day", $"magnitude", $"value")
  }

  private def calculateDateRanges(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._
    df.groupBy($"measuring_point").agg(
      functions.min(col("day")).as("min_date"),
      functions.max(col("day")).as("max_date")
    )
  }

  private def generateTimeRanges(spark: SparkSession, start: Long, stop: Long, dateDF: DataFrame): DataFrame = {
    import spark.implicits._
    val ranges = spark.range(start, stop, Duration.ofMinutes(15).toMillis)
    val dateRangesDF = ranges.select(col("id").alias("day_range"))
    dateRangesDF.crossJoin(dateDF)
      .filter($"min_date" <= $"day_range" && $"day_range" <= $"max_date")
      .withColumnRenamed("measuring_point", "measuring_point_range")
  }

  private def aggregateOmData(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._
    df.groupBy($"measuring_point", $"day")
      .pivot($"magnitude")
      .max("value")
  }

  private def formatOutputData(spark: SparkSession, rangeDF: DataFrame, aggregateDF: DataFrame): DataFrame = {
    import spark.implicits._

    var omWithNaNDF = rangeDF.join(aggregateDF, rangeDF("measuring_point_range") === aggregateDF("measuring_point") &&
      rangeDF("day_range") === aggregateDF("day"), "left")
      .withColumn("day_range", ($"day_range" / 1000).cast(IntegerType))

    // Check if column "1" exists before renaming
    if (omWithNaNDF.columns.contains("1")) {
      omWithNaNDF = omWithNaNDF.withColumnRenamed("1", "active_energy")
    } else {
      omWithNaNDF = omWithNaNDF.withColumn("active_energy", lit(-1))
    }

    // Check if column "3" exists before renaming
    if (omWithNaNDF.columns.contains("3")) {
      omWithNaNDF = omWithNaNDF.withColumnRenamed("3", "reactive_energy_1")
    } else {
      omWithNaNDF = omWithNaNDF.withColumn("reactive_energy_1", lit(-1))
    }

    // Fill NaN for only existing columns
    val naFillColumns = Seq("active_energy", "reactive_energy_1").filter(omWithNaNDF.columns.contains)
    omWithNaNDF = omWithNaNDF.na.fill("NaN", naFillColumns)

    omWithNaNDF.select($"measuring_point_range", $"day_range", $"active_energy", $"reactive_energy_1")
      .withColumnRenamed("measuring_point_range", "measuring_point")
      .withColumnRenamed("day_range", "day")
      .orderBy($"measuring_point", $"day")
  }



  private def saveToHDFS(df: DataFrame, folder: String): Unit = {
    val dateFormat = new SimpleDateFormat("yyyyMMddHHmm")
    dateFormat.setTimeZone(TimeZone.getTimeZone("GMT-3")) // Set Uruguay timezone
    val currentDateTime = dateFormat.format(new Date())
    val outputPath = s"hdfs:////user/deptorecener/alexOutputs/$currentDateTime/omDAICE/$folder/"

    df.coalesce(1)
      .write.mode(SaveMode.Overwrite)
      .options(Map("header" -> "false", "delimiter" -> ","))
      .csv(outputPath)
  }

  def convertScanToString(scan: Scan): String = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.getEncoder.encodeToString(proto.toByteArray)
  }
}
