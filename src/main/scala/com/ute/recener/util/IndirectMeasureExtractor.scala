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
import org.apache.spark.sql.{SaveMode, SparkSession, functions}

import java.time.Duration
import java.util.{ArrayList, Base64}


object IndirectMeasureExtractor{


  final val cfDataIV = "IV"
  final val cfDataOM = "OM"
  final val cfDataBytes = Bytes.toBytes(cfDataIV)
  private val serialVersionUID = 1L

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "recener")


    val conf = new SparkConf()
      .setAppName("Indirect measure Extractor")
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

    val start: Long = args(0).toLong
    val stop: Long = args(1).toLong
    val directory: String = args(2) //example:  user/deptorecener/indirectMeasure

    val date1 = "%014d".format(start)
    val date2 = "%014d".format(stop)

    val findIndirectMeasuringPointsSql = Inventory.findIndirectMeasuringPoints()
    val measuringPointDf = spark.read.jdbc(jdbcUrl, findIndirectMeasuringPointsSql, connectionProperties)
    val measuringPointIds: List[String] = measuringPointDf.select("ID").map(_.toString().replace("[", "").replace("]", "")).collect().toList

    if (measuringPointIds.length > 0) {

      exampleHConf.set(TableInputFormat.INPUT_TABLE, "MDM_DATA:IV")
      exampleHConf.set(TableInputFormat.SCAN, filterByMultiRowRangeIV(measuringPointIds, date1, date2))


      val hBaseRDIV: RDD[(ImmutableBytesWritable, Result)] = spark
        .sparkContext
        .newAPIHadoopRDD(
          exampleHConf,
          classOf[TableInputFormat],
          classOf[ImmutableBytesWritable],
          classOf[Result]
        )

      /*Getting Instantaneous values*/
      val resultRDDIV = hBaseRDIV.map(tuple => tuple._2)
      val ivRDD = resultRDDIV.map(x => MappingSchema.parseIvRow(x, cfDataIV))

      /*
        1	Potencia Activa Total
        2	Potencia Reactiva Total
        3	Factor de Potencia Total
        4	Potencia Activa Fase 1
        5	Potencia Reactiva Fase 1
        6	Factor de Potencia Fase 1
        7	Potencia Activa Fase 2
        8	Potencia Reactiva Fase 2
        9	Factor de Potencia Fase 2
        10	Potencia Activa Fase 3
        11	Potencia Reactiva Fase 3
        12	Factor de Potencia Fase 3
        13	Tensión Fase 1
        14	Intensidad Fase 1
        15	Tensión Fase 2
        16	Intensidad Fase 2
        17	Tensión Fase 3
        18	Intensidad Fase 3
        19	Angulo entre tension de fases L1 y L3
        20	Angulo entre tension de fases L2 y L1
        21	Angulo entre tension de fases L3 y L2
        22	Angulo entre tension y corriente L1
        23	Angulo entre tension y corriente L2
        24	Angulo entre tension y corriente L3
      */

      var magnitudes = Set("1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16",
        "17","18","19","20","21","22","23","24")

      //Filter instantaneous value by magnitude and measurement interval (to keep only instantaneous registers)
      val ivDF = ivRDD.toDF().filter($"magnitude".isInCollection(magnitudes))
        //.withColumn("date", to_date(to_utc_timestamp(from_unixtime((($"date") / 1000), "dd/MM/yyyy HH:mm"), "UTC")))
        .withColumn("date", (to_utc_timestamp(from_unixtime((($"date") / 1000), "yyyy-MM-dd HH:mm:ss"), "UTC")))
        .select($"measuring_point", $"date", $"magnitude", $"measurement_interval", $"value", $"source")
        .na.fill("INSTANTANEO", Seq("measurement_interval"))

      //ivDF.show()
      //println("Cantidad VI: " + ivDF.count())

      /*Getting Optimal measures*/

      /*
        1	Energía Activa Entrante (kWh)
        2	Energía Activa Saliente (kWh)
        3	Energía Reactiva Cuadrante 1 (kVArh)
        4	Energía Reactiva Cuadrante 2 (kVArh)
        5	Energía Reactiva Cuadrante 3 (kVArh)
        6	Energía Reactiva Cuadrante 4 (kVArh)
      */

      val mi = "1" //measurement_interval = 1 = QH
      val d1 = start / 3600000
      val dateOM1 = "%010d".format(d1)
      val d2 = stop / 3600000
      val dateOM2 = "%010d".format(d2)

      exampleHConf.set(TableInputFormat.INPUT_TABLE, "MDM_DATA:OM")
      exampleHConf.set(TableInputFormat.SCAN, filterByMultiRowRangeOM(measuringPointIds, dateOM1, dateOM2, mi))

      val hBaseRDOM: RDD[(ImmutableBytesWritable, Result)] = spark
        .sparkContext
        .newAPIHadoopRDD(
          exampleHConf,
          classOf[TableInputFormat],
          classOf[ImmutableBytesWritable],
          classOf[Result]
        )

      val resultRDDOM = hBaseRDOM.map(tuple => tuple._2)
      val omRDD = resultRDDOM.map(x => MappingSchema.parseOmRow(x, cfDataOM))
      val omDF = omRDD.toDF()
        .withColumn("dayDate", to_date(to_utc_timestamp(from_unixtime((($"day") / 1000), "yyyy-MM-dd HH:mm:ss"), "UTC")))
        .withColumn("DAY_TIME", upperUDF($"dayDate", $"period"))
        .withColumn("day", to_utc_timestamp($"DAY_TIME", "yyyy-MM-dd HH:mm:ss"))
        .withColumnRenamed("day", "date")
        .select($"measuring_point", $"date", $"magnitude", $"measurement_interval", $"value", $"source")

      //omDF.show()
      //println("Cantidad OM: " + omDF.count())

      val allTogether = ivDF.union(omDF)

      //allTogether.show()
      //println("Cantidad allTogether: " + allTogether.count())

      val findIndirectMeasuringPointsInfoSql = Inventory.findIndirectMeasuringPointsInfo()
      val measuringPointInfoDf = spark.read.jdbc(jdbcUrl, findIndirectMeasuringPointsInfoSql, connectionProperties)

      //measuringPointInfoDf.show()

      val allInfo = measuringPointInfoDf.join(allTogether, measuringPointInfoDf("ID") === allTogether("measuring_point")
        && measuringPointInfoDf("SA_START_DATE") < allTogether("date")
        && (allTogether("date") <= measuringPointInfoDf("SA_END_DATE") || col("SA_END_DATE").isNull)
        && measuringPointInfoDf("TARIFF_STRUCTURE_START_DATE") < allTogether("date")
        && (allTogether("date") <= measuringPointInfoDf("TARIFF_STRUCTURE_END_DATE") || col("TARIFF_STRUCTURE_END_DATE").isNull))

      //allInfo.show()
      //println("Cantidad allInfo: " + allInfo.count())

      val findIndirectMeasuringPointsEquipmentsSql = Inventory.findIndirectMeasuringPointsEquipments()
      val measuringPointEquipmentsDf = spark.read.jdbc(jdbcUrl, findIndirectMeasuringPointsEquipmentsSql, connectionProperties)

      //measuringPointEquipmentsDf.show()

      val allInfoWithEquipment = allInfo.join(measuringPointEquipmentsDf, allInfo("ID") === measuringPointEquipmentsDf("ID")
        && measuringPointEquipmentsDf("EQUIP_START_DATE") < allInfo("date")
        && (allInfo("date") <= measuringPointEquipmentsDf("EQUIP_END_DATE") || col("EQUIP_END_DATE").isNull))

      //allInfoWithEquipment.show()
      //println("Cantidad allInfoWithEquipment: " + allInfoWithEquipment.count())

      val findSourcesSql = Inventory.findSources()
      val sourcesDf = spark.read.jdbc(jdbcUrl, findSourcesSql, connectionProperties)

      val findMagnitudesSql = Inventory.findMagnitudes()
      val magnitudesDf = spark.read.jdbc(jdbcUrl, findMagnitudesSql, connectionProperties)

      val allInfoWithEquipmentSources = allInfoWithEquipment.join(sourcesDf, allInfoWithEquipment("source") === sourcesDf("SOURCE_ID"))
      val allInfoWithEquipmentMagnitudes = allInfoWithEquipmentSources.join(magnitudesDf, allInfoWithEquipmentSources("magnitude") === magnitudesDf("MAGNITUDE_ID"))
        .withColumn("measurement_interval", when(($"measurement_interval" === 1), "QH").otherwise($"measurement_interval"))

      //allInfoWithEquipmentMagnitudes.show()

      val formattedDF = allInfoWithEquipmentMagnitudes
        .select($"CODE", $"MANAGEMENT", $"OFFICE", $"TARIFF_STRUCTURE", $"date", $"DESCRIPTION", $"measurement_interval",
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

      //formattedDF.show()

//      formattedDF.coalesce(1).
//        write.mode(SaveMode.Overwrite).
//        options(Map("header" -> "true", "delimiter" -> ","))
//        .csv("hdfs:////"+ directory)

      val numberOfBatches = 1000  // Adjust this number to change the batch size
      formattedDF
        .repartition(numberOfBatches)
        .write
        .mode(SaveMode.Overwrite)
        .options(Map("header" -> "true", "delimiter" -> ","))
        .csv("hdfs:////" + directory)

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
  def   formatDateTime(date: String, period: Int): String = {

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
  val upperUDF = udf { (d: String, p: Int) => formatDateTime(d, p) }

}