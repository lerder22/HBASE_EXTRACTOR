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
import java.util.{ArrayList, Base64, Date, Properties, TimeZone}
import scala.collection.mutable.ListBuffer

/////////////////////////////


object IndirectMeasureExtractorOptimized {
  final val cfDataIV = "IV"
  final val cfDataOM = "OM"
  final val cfDataBytes = Bytes.toBytes(cfDataIV)
  private val serialVersionUID = 1L

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "recener")
    val conf = new SparkConf().setAppName("Indirect measure Extractor")
    val jdbcUrl = DataBaseConnection.getUrl
    val connectionProperties = DataBaseConnection.getConnectionProperties
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val exampleHConf = HBaseConfiguration.create()
    exampleHConf.set("hbase.zookeeper.quorum", "mdmprdmgm.corp.ute.com.uy,mdmprdhed1.corp.ute.com.uy,mdmprdhed2.corp.ute.com.uy")

    // Parse arguments
    val dateFormat = new SimpleDateFormat("yyyyMMddHHmm")
    dateFormat.setTimeZone(TimeZone.getTimeZone("GMT-3")) // Set Uruguay timezone

    val startString = args(0)
    val stopString = args(1)

    val start = dateFormat.parse(startString).getTime
    val stop = dateFormat.parse(stopString).getTime

    val directory: String = args(2)
    val chunkSize = args(3).toInt

    val date1 = "%014d".format(start)
    val date2 = "%014d".format(stop)



    // Suponiendo que measuringPointIds es una List[String] con 16000 IDs
    val measuringPointIds = getMeasuringPointIds(jdbcUrl, connectionProperties, spark)

    if (measuringPointIds.nonEmpty) {

      // Dividir measuringPointIds en chunks de 1000
      val chunks = measuringPointIds.grouped(chunkSize)

      val dateFormat = new SimpleDateFormat("yyyyMMddHHmm")
      val currentDateTime = dateFormat.format(new Date())
      val outputPath = s"hdfs:////user/deptorecener/alexOutputs/ivDAICE/$currentDateTime/"

      // Procesar cada chunk
      chunks.foreach { chunk =>
        val ivDF = extractIVData(exampleHConf, chunk, date1, date2, spark)
        val omDF = extractOMData(exampleHConf, start, stop, chunk, spark)
        val unionDF = ivDF.unionByName(omDF)

        // Realizar cualquier transformación adicional necesaria antes de guardar
        val processedDF = unionDF
          .transform(joinWithMeasuringPointInfo(_, jdbcUrl, connectionProperties, spark))
          .transform(joinWithMeasuringPointEquipments(_, jdbcUrl, connectionProperties, spark))
          .transform(joinWithSources(_, jdbcUrl, connectionProperties, spark))
          .transform(joinWithMagnitudes(_, jdbcUrl, connectionProperties, spark))
          .transform(formatDataFrame(_, spark))

        // Guardar o anexar el DataFrame procesado en el archivo
        processedDF.coalesce(1).
          write.mode(SaveMode.Append).
          options(Map("header" -> "true", "delimiter" -> ";")).
          csv(outputPath)
      }
    }

  }

  private def getMeasuringPointIds(jdbcUrl: String, connectionProperties: Properties, spark: SparkSession): List[String] = {
    import spark.implicits._

    val findIndirectMeasuringPointsSql = Inventory.findIndirectMeasuringPoints()
    val measuringPointDf = spark.read.jdbc(jdbcUrl, findIndirectMeasuringPointsSql, connectionProperties)
    measuringPointDf.select("ID").map(_.toString.replace("[", "").replace("]", ""))
      .collect().toList

  }

  private def extractIVData(hBaseConf: Configuration, measuringPointIds: List[String], startDate: String, endDate: String, spark: SparkSession): DataFrame = {
    import spark.implicits._

    hBaseConf.set(TableInputFormat.INPUT_TABLE, "MDM_DATA:IV")
    hBaseConf.set(TableInputFormat.SCAN, filterByMultiRowRangeIV(measuringPointIds, startDate, endDate))

    val hBaseRDD: RDD[(ImmutableBytesWritable, Result)] = spark.sparkContext.newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    val resultRDD = hBaseRDD.map(tuple => tuple._2)
    val ivRDD = resultRDD.map(x => MappingSchema.parseIvRow(x, cfDataIV))

    val magnitudes = Set("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24")
    ivRDD.toDF().filter($"magnitude".isInCollection(magnitudes))
      .withColumn("date", (to_utc_timestamp(from_unixtime((($"date") / 1000), "yyyy-MM-dd HH:mm:ss"), "UTC")))
      .select($"measuring_point", $"date", $"magnitude", $"measurement_interval", $"value", $"source")
      .na.fill("INSTANTANEO", Seq("measurement_interval"))
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

  private def extractOMData(hBaseConf: Configuration, start: Long, stop: Long, measuringPointIds: List[String], spark: SparkSession): DataFrame = {
    import spark.implicits._

    val mi = "1" // Measurement interval = 1 = QH
    val d1 = start / 3600000
    val dateOM1 = "%010d".format(d1)
    val d2 = stop / 3600000
    val dateOM2 = "%010d".format(d2)

    hBaseConf.set(TableInputFormat.INPUT_TABLE, "MDM_DATA:OM")
    hBaseConf.set(TableInputFormat.SCAN, filterByMultiRowRangeOM(measuringPointIds, dateOM1, dateOM2, mi))

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
