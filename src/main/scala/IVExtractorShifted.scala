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
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}

import java.time.{Duration, LocalDate, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.{ArrayList, Base64}


object IVExtractorShifted {


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

    val jdbcUrl = DataBaseConnection.getUrl
    val connectionProperties = DataBaseConnection.getConnectionProperties

    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    @transient val exampleHConf = HBaseConfiguration.create()
    exampleHConf.set("hbase.zookeeper.quorum", "mdmprdmgm.corp.ute.com.uy,mdmprdhed1.corp.ute.com.uy,mdmprdhed2.corp.ute.com.uy")

    val format = new java.text.SimpleDateFormat("yyyy-MM-dd")

    val directory: String = args(0) //example  user/deptorecener/alexOutputs/20230123

    // phase = 0 --> monophasic
    // phase = 1 --> triphasic
    for (phase <- 1 to 1) {

      //val findAllMeasuringPointsSql = Inventory.findMeasuringPointsByGroupAndPhase(group, phase)
      val findAllMeasuringPointsSql = Inventory.getInspectedMeasuringPointsWithDates(21)
      val measuringPointDf = spark.read.jdbc(jdbcUrl, findAllMeasuringPointsSql, connectionProperties)

      val PUNTO_MEDIDA_MDM = "PUNTO_MEDIDA_MDM"
      val PUNTO_SERVICIO = "PUNTO_SERVICIO"
      val FECHAI1 = "FECHAI1"

      // Select the ID and DATE columns, and convert DATE from 'dd/MM/yyyy' to 'yyyyMMdd' format
      val formattedDf = measuringPointDf
        .select(col(PUNTO_MEDIDA_MDM), col(PUNTO_SERVICIO), to_date(col(FECHAI1), "dd/MM/yyyy").as("DATE")) // Convert string to date
        .withColumn("FORMATTED_DATE", date_format($"DATE", "yyyyMMdd")) // Format date to 'yyyyMMdd'
        .select(col(PUNTO_MEDIDA_MDM), col(PUNTO_SERVICIO), $"FORMATTED_DATE".as(FECHAI1))

      def filterByServicePointAndDateIV(df: DataFrame): String = {
        val scan = new Scan()
        val ranges = new ArrayList[RowRange]()
        val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")

        df.collect().foreach(row => {
          val pm = row.getAs[java.math.BigDecimal](fieldName = PUNTO_MEDIDA_MDM).toString
          val dateStr = row.getAs[String](fieldName = FECHAI1)
          val date = LocalDate.parse(dateStr, formatter)

          // Start of the day in milliseconds since Unix epoch
          val dateInMilliseconds = date.atStartOfDay(ZoneId.systemDefault()).toInstant.toEpochMilli
          // Convert to hours and format
          val endInHours = "%010d".format(dateInMilliseconds / (1000 * 60 * 60))

          // Calculate startDate by subtracting 90 days and converting to hours
          val startDate = date.minusDays(90)
          val startInMilliseconds = startDate.atStartOfDay(ZoneId.systemDefault()).toInstant.toEpochMilli
          val startInHours = "%010d".format(startInMilliseconds / (1000 * 60 * 60))

          val startRowKey = pm.reverse.padTo(8, '0').reverse + startInHours + "00000"
          val endRowKey   = pm.reverse.padTo(8, '0').reverse + endInHours   + "99999"
          ranges.add(new RowRange(Bytes.toBytes(startRowKey), true, Bytes.toBytes(endRowKey), true))
        })

        val filter = new MultiRowRangeFilter(ranges)
        scan.setFilter(filter)
        //scan
        convertScanToString(scan)
      }

      exampleHConf.set(TableInputFormat.INPUT_TABLE, "MDM_DATA:IV")
      exampleHConf.set(TableInputFormat.SCAN, filterByServicePointAndDateIV(formattedDf))

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
//      val ranges = spark.range(start, stop, Duration.ofMinutes(15).toMillis)
//      val dateRangesDF = ranges.select(col("id").alias("date_range"))
//      val rangesPerMeasuringPoint = dateRangesDF.crossJoin(ivDatesDF).filter($"min_date" <= $"date_range" && $"date_range" <= $"max_date")
//        .withColumnRenamed("measuring_point", "measuring_point_range")

      // ----------- START ----------- //

      // Convert FECHAI1 to DATE and calculate start_date as 3 months before FECHAI1
      val formattedDf2 = measuringPointDf
        .withColumn("DATE", to_date(col("FECHAI1"), "dd/MM/yyyy"))
        .withColumn("FORMATTED_DATE", date_format(col("DATE"), "yyyyMMdd").cast(IntegerType))
        .withColumn("stop_date", col("DATE"))
        .withColumn("start_date", date_sub(col("DATE"), 90))
        .select(col("PUNTO_MEDIDA_MDM").as("measuring_point"), col("PUNTO_SERVICIO"), col("start_date"), col("stop_date"))

      // Define a UDF to generate ranges
      val generateRanges = udf((start: Long, stop: Long) => {
        (start to stop by Duration.ofMinutes(15).toMillis).map(_.toString)
      })

      // Add the date ranges
      val formattedDfWithRanges = formattedDf2
        .withColumn("date_ranges", generateRanges(
          unix_timestamp(col("start_date")) * 1000,
          unix_timestamp(col("stop_date")) * 1000
        ))

      // Explode the date ranges into separate rows
      val explodedRangesDf = formattedDfWithRanges
        .withColumn("date_range", explode(col("date_ranges")))
        .withColumn("date_range", (col("date_range") / 1000).cast(IntegerType)) // convert to integer seconds
        .select("measuring_point", "date_range")

      val ivMagnitudesToColumnDF = ivDF.groupBy($"measuring_point", $"date").pivot($"magnitude").max("value")

      val ivWithNaNDF = explodedRangesDf.join(ivMagnitudesToColumnDF,
          explodedRangesDf("measuring_point") === ivMagnitudesToColumnDF("measuring_point") &&
            explodedRangesDf("date_range") === (ivMagnitudesToColumnDF("date") / 1000).cast(IntegerType), "left"
        )
        .select(
          explodedRangesDf("measuring_point"),
          explodedRangesDf("date_range"),
          ivMagnitudesToColumnDF("*")
        )



      // ----------- END   ----------- //

      val ivMagnitudesToColumnDF = ivDF.groupBy($"measuring_point", $"date").pivot($"magnitude").max("value")

//      val ivWithNaNDF = rangesPerMeasuringPoint.join(ivMagnitudesToColumnDF, rangesPerMeasuringPoint("measuring_point_range") === ivMagnitudesToColumnDF("measuring_point") && rangesPerMeasuringPoint("date_range") === ivMagnitudesToColumnDF("date"), "left")
//        .withColumn("date_range", ($"date_range" / 1000).cast(IntegerType))

      val ivWithNaNDF = ivMagnitudesToColumnDF

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

      }

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