import com.ute.recener.util.{DataBaseConnection, Inventory, MappingSchema}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.filter.{FuzzyRowFilter, MultiRowRangeFilter}
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Bytes, Pair}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{SaveMode, SparkSession, functions}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.DataFrame

import java.time.Duration
import java.util.{ArrayList, Base64, Calendar, Date}
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import java.text.SimpleDateFormat
import java.util.TimeZone
import org.apache.log4j.Logger

object OMDaiceExtractorV2 {

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
    exampleHConf.set("hbase.zookeeper.quorum", "mdmprdmgm1.corp.ute.com.uy,mdmprdhed1.corp.ute.com.uy,mdmprdhed2.corp.ute.com.uy")

    // Initialize JDBC
    val jdbcUrl = DataBaseConnection.getUrl
    val connectionProperties = DataBaseConnection.getConnectionProperties

    // Parse arguments
    val group = args(0)
    val dateFormat = new SimpleDateFormat("yyyyMMddHHmm")
    dateFormat.setTimeZone(TimeZone.getTimeZone("GMT-3")) // Set Uruguay timezone

    val startString = args(1)
    val stopString = args(2)

    val start = dateFormat.parse(startString).getTime
    val stop = dateFormat.parse(stopString).getTime

    // Log the parsed arguments
    logger.info(s"Parsed Arguments: ")
    logger.info(s"Group: $group")
    logger.info(s"Start String: $startString")
    logger.info(s"Stop String: $stopString")
    logger.info(s"Parsed Start Time: $start")
    logger.info(s"Parsed Stop Time: $stop")

    // Generate date strings
    val date1 = "%010d".format(start / 3600000)
    val date2 = "%010d".format(stop / 3600000)
    val measureInterval = "1"

    // Fuzzy filter parameters
    val startDate = Calendar.getInstance()
    startDate.setTime(new Date(start))
    val endDate = Calendar.getInstance()
    endDate.setTime(new Date(stop))
    val activeEnergy = "1"
    val q1 = "3"
    val measureMagnitude: Set[String] = Set(activeEnergy, q1)

    // Fetch Data from JDBC
    val findAllMeasuringPointsSql = Inventory.findMeasuringPointsByGroup(group)
    val measuringPointDf = spark.read.jdbc(jdbcUrl, findAllMeasuringPointsSql, connectionProperties)
    val measuringPointIds = measuringPointDf.select("ID").map(_.toString().replace("[", "").replace("]", "")).collect().toList

    // Log and check the size
    logger.info(s"Size of measuringPointDf: ${measuringPointDf.count}")
    if (measuringPointIds.isEmpty) {
      logger.info("measuringPointIds is empty. Exiting.")
      return
    }

    // Check if measuringPointIds is not empty
    if (measuringPointIds.nonEmpty) {

      // Configure HBase table and filters
      configureHBaseInput(exampleHConf, "MDM_DATA:OM", fuzzyFilterList(startDate, endDate, measureMagnitude, measureInterval))

      // Read data from HBase into an RDD
      val hBaseRDD = readFromHBase(spark, exampleHConf)

      // Log and check the size of hBaseRDD, parsedRDD, etc.
      logger.info(s"Size of hBaseRDD: ${hBaseRDD.count}")
      if (hBaseRDD.isEmpty()) {
        logger.info("hBaseRDD is empty. Exiting.")
        return
      }

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

      // Log and check the size of measuringPointDf and show first 5 rows
      logger.info(s"Size of measuringPointDf: ${measuringPointDf.count}")
      measuringPointDf.show(5)

      if (measuringPointDf.count == 0) {
        logger.info("measuringPointDf is empty. Exiting.")
        return
      }

      // Log and check the size of rangesPerMeasuringPoint and show first 5 rows
      logger.info(s"Size of rangesPerMeasuringPoint: ${rangesPerMeasuringPoint.count}")
      rangesPerMeasuringPoint.show(5)

      if (rangesPerMeasuringPoint.count == 0) {
        logger.info("rangesPerMeasuringPoint is empty. Exiting.")
        return
      }

      // Log and check the size of omAggregatedDF and show first 5 rows
      logger.info(s"Size of omAggregatedDF: ${omAggregatedDF.count}")
      omAggregatedDF.show(5)

      if (omAggregatedDF.count == 0) {
        logger.info("omAggregatedDF is empty. Exiting.")
        return
      }

      // Format the output DataFrame
      val finalDF = formatOutputData(spark, rangesPerMeasuringPoint, omAggregatedDF, measuringPointDf)

      // Save the final DataFrame to HDFS
      saveToHDFS(finalDF)
    }
  }

  case class FuzzyData(rowKeyPattern: String, maskInfo: String)

  private object Constants {
    val MaskInfo = "\\x01\\x01\\x01\\x01\\x01\\x01\\x01\\x01\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00"
    val TimeDivider = 3600000
  }

  def fuzzyFilterList(start: Calendar, end: Calendar, magnitudes: Set[String], measurementInfo: String): Seq[FuzzyData] = {
    import Constants._

    val listFuzzy = ListBuffer[FuzzyData]()
    var tempStart = start.clone().asInstanceOf[Calendar]

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

  private def formatOutputData(spark: SparkSession, rangeDF: DataFrame, aggregateDF: DataFrame,
                               measuringPointDf: DataFrame): DataFrame = {
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

    omWithNaNDF.join(measuringPointDf, omWithNaNDF("measuring_point_range") === measuringPointDf("ID"))
      .select($"CODE", $"day_range", $"active_energy", $"reactive_energy_1")
      .withColumnRenamed("CODE", "measuring_point")
      .withColumnRenamed("day_range", "day")
      .orderBy($"measuring_point", $"day")

  }


  private def saveToHDFS(df: DataFrame): Unit = {
    val dateFormat = new SimpleDateFormat("yyyyMMddHHmm")
    val currentDateTime = dateFormat.format(new Date())
    val outputPath = s"hdfs:////user/deptorecener/alexOutputs/$currentDateTime/omDAICE/"

    df.coalesce(1)
      .write.mode(SaveMode.Overwrite)
      .options(Map("header" -> "false", "delimiter" -> ","))
      .csv(outputPath)
  }

  def convertScanToString(scan: Scan): String = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.getEncoder.encodeToString(proto.toByteArray)
  }

  def filterByMultiRowRangeOM(list: List[String], startDate: String, stopDate: String, measureInterval: String): String = {
    val scan = new Scan()
    val ranges = new ArrayList[RowRange]()

    list.foreach(pm => {
      val startRowKey = pm.reverse.padTo(8, '0').reverse + measureInterval + startDate + "00000"
      val endRowKey = pm.reverse.padTo(8, '0').reverse + measureInterval + stopDate + "99999"
      ranges.add(new RowRange(Bytes.toBytes(startRowKey), true, Bytes.toBytes(endRowKey), true))
    })

    val filter = new MultiRowRangeFilter(ranges)
    scan.setFilter(filter)
    //scan
    convertScanToString(scan)
  }

}