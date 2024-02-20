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


/////////////////////////////


object OMInspectionsExtractor {
  final val cfDataOM = "OM"
  private val serialVersionUID = 1L

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "recener")
    val conf = new SparkConf().setAppName("OM Inspections Extractor")
    val jdbcUrl = DataBaseConnection.getUrl
    val connectionProperties = DataBaseConnection.getConnectionProperties
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val exampleHConf = HBaseConfiguration.create()
    exampleHConf.set("hbase.zookeeper.quorum", "mdmprdmgm.corp.ute.com.uy,mdmprdhed1.corp.ute.com.uy,mdmprdhed2.corp.ute.com.uy")

    // Suponiendo que measuringPointDF es un DataFrame con 360k filas
    val measuringPointDF = getMeasuringPointWithDatesDF(jdbcUrl, connectionProperties, spark)

    val dateFormat = new SimpleDateFormat("yyyyMMddHHmm")

    val currentDateTime = dateFormat.format(new Date())

    val outputPath = s"hdfs:////user/deptorecener/alexOutputs/omDAICEshifted/$currentDateTime/"

    // Procesar cada chunk
    val omDF = extractOMData(exampleHConf, measuringPointDF, spark)

    // Guardar o anexar el DataFrame procesado en el archivo
    omDF.coalesce(1).
      write.mode(SaveMode.Append).
      options(Map("header" -> "true", "delimiter" -> ";")).
      csv(outputPath)
  }

  private def getMeasuringPointWithDatesDF(jdbcUrl: String, connectionProperties: Properties, spark: SparkSession): DataFrame = {
    import spark.implicits._

    // Assuming Inventory.findInspectedMeasuringPoints() returns a SQL query that includes a DATE column in 'dd/MM/yyyy' format
    val findInspectedMeasuringPointsSql = Inventory.findInspectedMeasuringPoints()

    // Read the data from your database
    val measuringPointDf = spark.read.jdbc(jdbcUrl, findInspectedMeasuringPointsSql, connectionProperties)

    // Select the ID and DATE columns, and convert DATE from 'dd/MM/yyyy' to 'yyyyMMdd' format
    val formattedDf = measuringPointDf
      .select($"PUNTO_SERVICIO", to_date($"FECHAI1", "dd/MM/yyyy").as("DATE")) // Convert string to date
      .withColumn("FORMATTED_DATE", date_format($"DATE", "yyyyMMdd")) // Format date to 'yyyyMMdd'
      .select($"PUNTO_SERVICIO".as("PM"), $"FORMATTED_DATE".as("DATE")) // Rename columns as needed for filterByMultiRowRangeOM function

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

  private def extractOMData(hBaseConf: Configuration, measuringPointDF: DataFrame, spark: SparkSession): DataFrame = {
    import spark.implicits._

    val mi = "1" // Measurement interval = 1 = QH

    hBaseConf.set(TableInputFormat.INPUT_TABLE, "MDM_DATA:OM")
    hBaseConf.set(TableInputFormat.SCAN, filterByServicePointOM(measuringPointDF, mi))

    val hBaseRDD: RDD[(ImmutableBytesWritable, Result)] = spark.sparkContext.newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    val resultRDD = hBaseRDD.map(tuple => tuple._2)
    val omRDD = resultRDD.map(x => MappingSchema.parseOmRow(x, cfDataOM))

    omRDD.toDF()
      .withColumn("dayDate", to_date(to_utc_timestamp(from_unixtime((($"day") / 1000), "yyyy-MM-dd HH:mm:ss"), "UTC")))
      .withColumn("DAY_TIME", upperUDF($"dayDate", $"period"))
      .withColumn("day", to_utc_timestamp($"DAY_TIME", "yyyy-MM-dd HH:mm:ss"))
      .withColumnRenamed("day", "date")
      .select($"measuring_point", $"date", $"magnitude", $"measurement_interval", $"value", $"source")
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

  private def joinWithMeasuringPointInfo(df: DataFrame, jdbcUrl: String, connectionProperties: Properties, spark: SparkSession): DataFrame = {
    val findIndirectMeasuringPointsInfoSql = Inventory.findIndirectMeasuringPointsInfo()
    val measuringPointInfoDf = spark.read.jdbc(jdbcUrl, findIndirectMeasuringPointsInfoSql, connectionProperties)
    df.join(measuringPointInfoDf, df("measuring_point") === measuringPointInfoDf("ID")
      && measuringPointInfoDf("SA_START_DATE") < df("date")
      && (df("date") <= measuringPointInfoDf("SA_END_DATE") || col("SA_END_DATE").isNull)
      && measuringPointInfoDf("TARIFF_STRUCTURE_START_DATE") < df("date")
      && (df("date") <= measuringPointInfoDf("TARIFF_STRUCTURE_END_DATE") || col("TARIFF_STRUCTURE_END_DATE").isNull))
  }

  private def joinWithMeasuringPointEquipments(df: DataFrame, jdbcUrl: String, connectionProperties: Properties, spark: SparkSession): DataFrame = {
    val findIndirectMeasuringPointsEquipmentsSql = Inventory.findIndirectMeasuringPointsEquipments()
    val measuringPointEquipmentsDf = spark.read.jdbc(jdbcUrl, findIndirectMeasuringPointsEquipmentsSql, connectionProperties)
    df.join(measuringPointEquipmentsDf, df("ID") === measuringPointEquipmentsDf("ID")
      && measuringPointEquipmentsDf("EQUIP_START_DATE") < df("date")
      && (df("date") <= measuringPointEquipmentsDf("EQUIP_END_DATE") || col("EQUIP_END_DATE").isNull))
  }

  private def joinWithSources(df: DataFrame, jdbcUrl: String, connectionProperties: Properties, spark: SparkSession): DataFrame = {
    val findSourcesSql = Inventory.findSources()
    val sourcesDf = spark.read.jdbc(jdbcUrl, findSourcesSql, connectionProperties)
    df.join(sourcesDf, df("source") === sourcesDf("SOURCE_ID"))
  }

  private def joinWithMagnitudes(df: DataFrame, jdbcUrl: String, connectionProperties: Properties, spark: SparkSession): DataFrame = {
    import spark.implicits._

    val findMagnitudesSql = Inventory.findMagnitudes()
    val magnitudesDf = spark.read.jdbc(jdbcUrl, findMagnitudesSql, connectionProperties)
    df.join(magnitudesDf, df("magnitude") === magnitudesDf("MAGNITUDE_ID"))
      .withColumn("measurement_interval", when(($"measurement_interval" === 1), "QH").otherwise($"measurement_interval"))
  }

  private def formatDataFrame(df: DataFrame, spark:SparkSession): DataFrame = {
    import spark.implicits._

    df.select($"CODE", $"MANAGEMENT", $"OFFICE", $"TARIFF_STRUCTURE", $"date", $"DESCRIPTION", $"measurement_interval",
      $"value", $"SOURCE_NAME", $"VOLTAGE_LEVEL", $"BADGE_NUMBER")
      .withColumnRenamed("CODE", "PM")
      .withColumnRenamed("MANAGEMENT", "GERENCIA")
      .withColumnRenamed("OFFICE", "OFICINA")
      .withColumnRenamed("TARIFF_STRUCTURE", "TARIFA")
      .withColumnRenamed("date", "FECHA")
      .withColumnRenamed("DESCRIPTION", "MAGNITUD")
      .withColumnRenamed("measurement_interval", "INTERVALO MEDIDA")
      .withColumnRenamed("value", "VALOR")
      .withColumnRenamed("SOURCE_NAME", "ORIGEN")
      .withColumnRenamed("VOLTAGE_LEVEL", "NIVEL TENSION")
      .withColumnRenamed("BADGE_NUMBER", "BN")
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
