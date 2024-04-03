import com.ute.recener.util.{DataBaseConnection, Inventory, MappingSchema}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.{SaveMode, SparkSession, functions, Row}
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, ZoneId}
import java.util.ArrayList
import org.apache.spark.sql.DataFrame
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.sql.functions.{col, min, max}
import java.time.Duration

import java.time.Duration
import java.util.{ArrayList, Base64}


object IVDaiceExtractorInspections {


  final val cfDataIV = "IV"
  final val cfDataBytes = Bytes.toBytes(cfDataIV)
  private val serialVersionUID = 1L

  private val voltage_phase_1 = "13"
  private val current_phase_1 = "14"
  private val voltage_phase_2 = "15"
  private val current_phase_2 = "16"
  private val voltage_phase_3 = "17"
  private val current_phase_3 = "18"


  def convertScanToString(scan: Scan): String = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.getEncoder.encodeToString(proto.toByteArray)
  }
  def filterByServicePointIV(rows: Iterator[Row], measureInterval: String): Scan = {
    val scan = new Scan()
    val ranges = new ArrayList[RowRange]()
    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")

    rows.foreach { row =>
      val pm = row.getAs[String]("PM")
      val dateStr = row.getAs[String]("DATE")
      val date = LocalDate.parse(dateStr, formatter)

      // Start of the day in milliseconds since Unix epoch
      val dateInMilliseconds = date.atStartOfDay(ZoneId.systemDefault()).toInstant.toEpochMilli

      // Calculate startDate by subtracting 30 days
      val startDate = date.minusDays(30)
      val startInMilliseconds = startDate.atStartOfDay(ZoneId.systemDefault()).toInstant.toEpochMilli

      val magnitude = "00" // Assuming a fixed magnitude for the example
      val startRowKey = f"${pm.toLong}%08d$startInMilliseconds%014d$magnitude%s$measureInterval%s00"
      val endRowKey = f"${pm.toLong}%08d$dateInMilliseconds%014d$magnitude%s$measureInterval%s99"

      ranges.add(new RowRange(Bytes.toBytes(startRowKey), true, Bytes.toBytes(endRowKey), true))
    }

  // Function to process each partition (chunk)
  def processPartition(rows: Iterator[Row], exampleHConf: Configuration, measureInterval: String, spark: SparkSession, cfDataIV: String, directory: String): Unit = {
    import spark.implicits._

    // Here, 'rows' represents a chunk of your DataFrame. You can process it as needed.
    // Convert rows to a list to reuse it without losing the iterator
    val rowsList = rows.toList

    // Check if the partition is not empty
    if (rowsList.nonEmpty) {

      // Use the filterByServicePointIV function to get the configured Scan
      val scan = filterByServicePointIV(rows, measureInterval)

      // Convert the Scan to a String representation or use it directly depending on your HBase configuration
      val scanAsString = convertScanToString(scan)

      exampleHConf.set(TableInputFormat.INPUT_TABLE, "MDM_DATA:IV")
      exampleHConf.set(TableInputFormat.SCAN, scanAsString)

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

      var tri_magnitudes = Set(voltage_phase_2, current_phase_2, voltage_phase_3, current_phase_3)
      magnitudes = magnitudes ++ tri_magnitudes

      //Filter instantaneous value by magnitude and measurement interval (to keep only instantaneous registers)
      val ivDF = ivRDD.toDF().filter(($"magnitude".isInCollection(magnitudes)) && ($"measurement_interval" === 1))
        .select($"measuring_point", $"date", $"magnitude", $"value")



      // Group the original DataFrame (ivDF) by 'measuring_point' and calculate the minimum and maximum date for each group
      val ivDatesDF = ivDF.groupBy("measuring_point")
        .agg(
          min("date").as("min_date"), // Find the earliest date in each group
          max("date").as("max_date")  // Find the latest date in each group
        )

      // Generate a DataFrame of time steps, each 15 minutes apart, within a specified range (from 'start' to 'stop')
      val dateRangesDF = spark.range(start, stop, Duration.ofMinutes(15).toMillis)
        .select(col("id").alias("date_range"))  // Rename the 'id' column to 'date_range'

      // Combine every 15-minute time step with every measuring point and its date range
      val rangesPerMeasuringPoint = dateRangesDF
        .crossJoin(ivDatesDF)  // Perform a cross join to associate each time step with every measuring point
        .filter(
          col("min_date") <= col("date_range") && col("date_range") <= col("max_date")  // Keep only the time steps within the min/max date range for each measuring point
        )
        .withColumnRenamed("measuring_point", "measuring_point_range")  // Rename 'measuring_point' for clarity or further processing


      val ivMagnitudesToColumnDF = ivDF.groupBy($"measuring_point", $"date").pivot($"magnitude").max("value")

      val ivWithNaNDF = rangesPerMeasuringPoint.join(ivMagnitudesToColumnDF, rangesPerMeasuringPoint("measuring_point_range") === ivMagnitudesToColumnDF("measuring_point") && rangesPerMeasuringPoint("date_range") === ivMagnitudesToColumnDF("date"), "left")
        .withColumn("date_range", ($"date_range" / 1000).cast(IntegerType))


      //triphasic
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

    }
  }

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "recener")


    val conf = new SparkConf()
      .setAppName("Instantaneous value DAICE Inspections Extractor")

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
    val findAllMeasuringPointsSql = Inventory.findInspectedTriphasicMeasuringPoints()
    val measuringPointDf = spark.read.jdbc(jdbcUrl, findAllMeasuringPointsSql, connectionProperties)
//    val measuringPointIds: List[String] = measuringPointDf.select("ID").map(_.toString().replace("[", "").replace("]", "")).collect().toList
    // Calculate the number of partitions needed based on your desired batch size
    val totalRows = measuringPointDf.count()
    val batchSize = 10000 // Your desired chunk size
    val numPartitions = (totalRows / batchSize).toInt + 1

    // Repartition the DataFrame to have approximately 'numPartitions' partitions
    val repartitionedDf = measuringPointDf.repartition(numPartitions)

    // Process each partition of the repartitioned DataFrame
    repartitionedDf.foreachPartition(partitionIterator => processPartition(partitionIterator))

    // Assuming you have your measuringPointIds as a List[String]
    val batchSize = 10000 // Number of IDs to process in each batch

    // Split the list into batches
    val batches = measuringPointIds.grouped(batchSize).toList

    batches.foreach(batch => {
      // Now 'batch' contains a subset of your measuringPointIds
      // Configure HBase scan for the current batch


      })





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



      val filter: Filter = new MultiRowRangeFilter(ranges)
      scan.setFilter(filter)
      scan
    }

  }
}