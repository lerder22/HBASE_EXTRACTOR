import com.ute.recener.util.{DataBaseConnection, Inventory, MappingSchema}
import org.apache.hadoop.conf.Configuration
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
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.text.SimpleDateFormat
import java.util.{ArrayList, Base64, Date, Properties}
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.ZoneId

import org.apache.spark.sql.types._


/////////////////////////////


object OMInspectionsExtractor {
  final val cfDataOM = "OM"
  private final val PUNTO_SERVICIO_MDM = "PUNTO_SERVICIO"
  private final val PUNTO_SERVICIO_CCB = "PUNTO_SERVICIO_CCB"

  private val serialVersionUID = 1L

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "recener")
    val conf = new SparkConf().setAppName("OM Inspections Extractor")

    // Default value for coalesce
    var numPartitions = 10 // Default value in case the argument is not provided

    // Check if the argument is provided and is a valid integer
    if (args.length > 0) {
      try {
        numPartitions = args(0).toInt
      } catch {
        case e: NumberFormatException => println("The first argument is not an integer. Using default value.")
      }
    }

    val jdbcUrl = DataBaseConnection.getUrl
    val connectionProperties = DataBaseConnection.getConnectionProperties
    val spark = SparkSession.builder().config(conf).getOrCreate()

//    val exampleHConf = HBaseConfiguration.create()
//    exampleHConf.set("hbase.zookeeper.quorum", "mdmprdmgm.corp.ute.com.uy,mdmprdhed1.corp.ute.com.uy,mdmprdhed2.corp.ute.com.uy")

    val dateFormat = new SimpleDateFormat("yyyyMMddHHmm")

    val currentDateTime = dateFormat.format(new Date())

    val outputPath = s"hdfs:////user/deptorecener/alexOutputs/omDAICEshifted/$currentDateTime/"

    // Suponiendo que measuringPointDF es un DataFrame con 360k filas
    val measuringPointDF = getMeasuringPointWithDatesDF(jdbcUrl, connectionProperties, spark)

    val numPartitionsDF = (measuringPointDF.count() / 10).toInt + 1

//    val partitionedDF = measuringPointDF.repartition(numPartitionsDF)

//    if(partitionedDF != null){
//      // Procesar cada chunk
      extractOMData(measuringPointDF, numPartitions, outputPath, spark)
//    }

//    spark.stop()

  }

  private def extractOMData(partitionedDF: DataFrame, numPartitions: Int, outputPath: String, spark: SparkSession): Unit = {
    import spark.implicits._

    val mi = "1" // Measurement interval = 1 = QH

    var measuringPointDF = partitionedDF
//    partitionedDF.foreachPartition { partitionIterator =>
//      if (partitionIterator.nonEmpty) {
        // Initialize the HBaseConfiguration inside the lambda function to avoid serialization issues
        val hBaseConfLocal = HBaseConfiguration.create()
        hBaseConfLocal.set("hbase.zookeeper.quorum", "mdmprdmgm.corp.ute.com.uy,mdmprdhed1.corp.ute.com.uy,mdmprdhed2.corp.ute.com.uy")
        hBaseConfLocal.set(TableInputFormat.INPUT_TABLE, "MDM_DATA:OM")

        // Define the schema based on your DataFrame's structure
        val schema = StructType(Seq(
          StructField("PM", StringType, true),
          StructField("PUNTO_SERVICIO_CCB", StringType, true),
          StructField("DATE", StringType, true) // Adjust types as needed
        ))
        // Convert the iterator to a Seq and then to a DataFrame
//        val rows = partitionIterator.filter(_ != null).toSeq // Filter out null rows
//        if (rows.nonEmpty) {
          // Create an RDD from the Seq of Rows
//          val rowsRDD = spark.sparkContext.parallelize(rows)

          // Create a DataFrame from the RDD and schema
//          val measuringPointDF = spark.createDataFrame(rowsRDD, schema)

          // Ensure filterByServicePointOM doesn't return null or cause an exception
          val scanConfig = Option(filterByServicePointOM(measuringPointDF, mi)).getOrElse("")
          hBaseConfLocal.set(TableInputFormat.SCAN, scanConfig)

          val hBaseRDD: RDD[(ImmutableBytesWritable, Result)] = spark.sparkContext.newAPIHadoopRDD(hBaseConfLocal, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
          val resultRDD = hBaseRDD.map(tuple => tuple._2)
          val omRDD = resultRDD.map(x => MappingSchema.parseOmRow(x, cfDataOM))

          var transformedDf = omRDD.toDF()
            .withColumn("dayDate", to_date(to_utc_timestamp(from_unixtime((($"day") / 1000), "yyyy-MM-dd HH:mm:ss"), "UTC")))
            .withColumn("DAY_TIME", upperUDF($"dayDate", $"period")) // Crea columna DAY_TIME
            .withColumn("day", to_utc_timestamp($"DAY_TIME", "yyyy-MM-dd HH:mm:ss"))
            .withColumnRenamed("day", "date")
            .select($"measuring_point", $"date", $"magnitude", $"measurement_interval", $"value", $"source")
            // Filter where magnitude is either "1" or "3" and source is "41"
            .filter($"magnitude".isin("1", "3") && $"source" === "41")

          // Group by 'measuring_point' and 'date', pivot on 'magnitude', and find the max 'value' for each group
          var omAggregatedDF = transformedDf
            .groupBy($"measuring_point", $"date")
            .pivot("magnitude")
            .max("value")

          // Now, 'resultDf' contains the desired transformation
          // Check if column "1" exists before renaming
          if (omAggregatedDF.columns.contains("1")) {
            omAggregatedDF = omAggregatedDF.withColumnRenamed("1", "active_energy")
          } else {
            omAggregatedDF = omAggregatedDF.withColumn("active_energy", lit(-1))
          }

          // Check if column "3" exists before renaming
          if (omAggregatedDF.columns.contains("3")) {
            omAggregatedDF = omAggregatedDF.withColumnRenamed("3", "reactive_energy_1")
          } else {
            omAggregatedDF = omAggregatedDF.withColumn("reactive_energy_1", lit(-1))
          }

//           Hacemos el join para cambiar el código de mdm por el punto de servicio
    omAggregatedDF = omAggregatedDF.join(
            measuringPointDF,
        omAggregatedDF("measuring_point") === measuringPointDF("PM")
          )
            .select(
            measuringPointDF(PUNTO_SERVICIO_CCB).alias("measuring_point"),
              omAggregatedDF("date"),
              omAggregatedDF("active_energy"),
              omAggregatedDF("reactive_energy_1")
          )


          // Guardar o anexar el DataFrame procesado en el archivo
          omAggregatedDF.coalesce(numPartitions)
            .write
            .mode(SaveMode.Append)
            .option("header", "true")
            .option("delimiter", ";")
            .csv(outputPath)


//        }
//      }
//    }
  }


  private def getMeasuringPointWithDatesDF(jdbcUrl: String, connectionProperties: Properties, spark: SparkSession): DataFrame = {
    import spark.implicits._

    // Assuming Inventory.findInspectedMeasuringPoints() returns a SQL query that includes a DATE column in 'dd/MM/yyyy' format
    val findInspectedMeasuringPointsSql = Inventory.findInspectedMeasuringPoints()

    // Read the data from your database
    val measuringPointDf = spark.read.jdbc(jdbcUrl, findInspectedMeasuringPointsSql, connectionProperties)

    // Select the ID and DATE columns, and convert DATE from 'dd/MM/yyyy' to 'yyyyMMdd' format
    val formattedDf = measuringPointDf
      .select(col(PUNTO_SERVICIO_MDM), col(PUNTO_SERVICIO_CCB), to_date($"FECHAI1", "dd/MM/yyyy").as("DATE")) // Convert string to date
      .withColumn("FORMATTED_DATE", date_format($"DATE", "yyyyMMdd")) // Format date to 'yyyyMMdd'
      .select(col(PUNTO_SERVICIO_MDM).as("PM"), col(PUNTO_SERVICIO_CCB), $"FORMATTED_DATE".as("DATE")) // Rename columns as needed for filterByMultiRowRangeOM function

    formattedDf
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

  private def filterByServicePointOM(df: DataFrame, measureInterval: String): String = {
    val scan = new Scan()
    val ranges = new ArrayList[RowRange]()
    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")

    df.collect().foreach(row => {
      val pm = row.getAs[java.math.BigDecimal]("PM").toString
      val dateStr = row.getAs[String]("DATE")
      val date = LocalDate.parse(dateStr, formatter)

      // Start of the day in milliseconds since Unix epoch
      val dateInMilliseconds = date.atStartOfDay(ZoneId.systemDefault()).toInstant.toEpochMilli
      // Convert to hours and format
      val endInHours = "%010d".format(dateInMilliseconds / (1000 * 60 * 60))

      // Calculate startDate by subtracting 90 days and converting to hours
      val startDate = date.minusDays(90)
      val startInMilliseconds = startDate.atStartOfDay(ZoneId.systemDefault()).toInstant.toEpochMilli
      val startInHours = "%010d".format(startInMilliseconds / (1000 * 60 * 60))

      val startRowKey = pm.reverse.padTo(8, '0').reverse + measureInterval + startInHours + "00000"
      val endRowKey = pm.reverse.padTo(8, '0').reverse + measureInterval + endInHours + "99999"
      ranges.add(new RowRange(Bytes.toBytes(startRowKey), true, Bytes.toBytes(endRowKey), true))
    })

    val filter = new MultiRowRangeFilter(ranges)
    scan.setFilter(filter)
    //scan
    convertScanToString(scan)
  }

  def saveToHDFS(df: DataFrame, directory: String): Unit = {
    val numberOfBatches = 1000  // Adjust this number to change the batch size

    val dateFormat = new SimpleDateFormat("yyyyMMddHHmm")
    val currentDateTime = dateFormat.format(new Date())
    val outputPath = s"hdfs:////user/deptorecener/alexOutputs/ivDAICE/$currentDateTime/"

    df.coalesce(1).
      write.mode(SaveMode.Append).
      options(Map("header" -> "true", "delimiter" -> ";")).
      csv(outputPath)

  }

  val upperUDF = udf { (d: String, p: Int) => formatDateTime(d, p) }

  def formatDateTime(date: String, period: Int): String = {
    var h = period / 4
    val m = (period % 4) * 15
    if (h == 24) {
      h = 0
    }
    val d = date.substring(0, 10)
    val hour = "%02d".format(h)
    val minut = "%02d".format(m)
    d + " " + hour + ":" + minut
  }

}
