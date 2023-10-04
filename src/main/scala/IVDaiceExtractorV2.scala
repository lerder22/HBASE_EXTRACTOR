//import com.ute.recener.util.{DataBaseConnection, Inventory, MappingSchema}
//import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
//import org.apache.hadoop.hbase.client._
//import org.apache.hadoop.hbase.filter.{MultiRowRangeFilter, MultiRowRangeFilter.RowRange}
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable
//import org.apache.hadoop.hbase.mapreduce.TableInputFormat
//import org.apache.hadoop.hbase.protobuf.ProtobufUtil
//import org.apache.hadoop.hbase.util.Bytes
//import org.apache.spark.SparkConf
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.{SaveMode, SparkSession}
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.types.{IntegerType, StringType}
//import java.util.{ArrayList, Base64}
//import java.time.Duration
//import scala.collection.JavaConversions._
//
//object IVDaiceExtractor {
//
//  final val ColumnFamilyData = "IV"
//  private val ColumnFamilyBytes = Bytes.toBytes(ColumnFamilyData)
//  private val ZookeeperQuorum = "mdmprdmgm1.corp.ute.com.uy,mdmprdhed1.corp.ute.com.uy,mdmprdhed2.corp.ute.com.uy"
//
//  def main(args: Array[String]): Unit = {
//    System.setProperty("HADOOP_USER_NAME", "recener")
//    val sparkConf = initializeSparkConf()
//    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
//    val hBaseConf = initializeHBaseConf()
//
//    val argsData = ArgsData(args)
//    processPhases(spark, hBaseConf, argsData)
//  }
//
//  case class ArgsData(group: String, start: Long, stop: Long, directory: String)
//
//  def initializeSparkConf(): SparkConf = {
//    new SparkConf().setAppName("Instantaneous value DAICE Extractor")
//  }
//
//  def initializeHBaseConf(): HBaseConfiguration = {
//    val conf = HBaseConfiguration.create()
//    conf.set("hbase.zookeeper.quorum", ZookeeperQuorum)
//    conf
//  }
//
//  def processPhases(spark: SparkSession, hBaseConf: HBaseConfiguration, argsData: ArgsData): Unit = {
//    for (phase <- 0 to 1) {
//      val measurePointIds = getMeasuringPointIds(spark, argsData, phase)
//      if (measurePointIds.nonEmpty) {
//        processMeasurePoints(spark, hBaseConf, argsData, phase, measurePointIds)
//      }
//    }
//  }
//
//  def getMeasuringPointIds(spark: SparkSession, argsData: ArgsData, phase: Int): List[String] = {
//    import spark.implicits._
//
//    val jdbcUrl = DataBaseConnection.getUrl
//    val connectionProperties = DataBaseConnection.getConnectionProperties
//    val findAllMeasuringPointsSql = Inventory.findMeasuringPointsBySource(21)
//
//    val df = spark.read.jdbc(jdbcUrl, findAllMeasuringPointsSql, connectionProperties)
//    df.select("ID")
//      .map(_.toString().replace("[", "").replace("]", ""))
//      .collect()
//      .toList
//  }
//
//  def processMeasurePoints(spark: SparkSession, hBaseConf: HBaseConfiguration, argsData: ArgsData, phase: Int, measurePointIds: List[String]): Unit = {
//    import spark.implicits._
//
//    hBaseConf.set(TableInputFormat.INPUT_TABLE, "MDM_DATA:IV")
//    hBaseConf.set(TableInputFormat.SCAN, filterByMultiRowRangeIV(measurePointIds, argsData.start, argsData.stop))
//
//    val hBaseRDD: RDD[(ImmutableBytesWritable, Result)] = spark.sparkContext.newAPIHadoopRDD(
//      hBaseConf,
//      classOf[TableInputFormat],
//      classOf[ImmutableBytesWritable],
//      classOf[Result]
//    )
//
//    val resultRDD = hBaseRDD.map(_._2)
//    val ivRDD = resultRDD.map(x => MappingSchema.parseIvRow(x, ColumnFamilyData))
//
//    // Further processing...
//    // ...
//  }
//
//  def filterByMultiRowRangeIV(list: List[String], startDate: Long, stopDate: Long): String = {
//    val scan = new Scan()
//    val ranges = new ArrayList[RowRange]()
//
//    list.foreach(pm => {
//      val startRowKey = generateRowKey(pm, startDate)
//      val endRowKey = generateRowKey(pm, stopDate)
//      ranges.add(new RowRange(Bytes.toBytes(startRowKey), true, Bytes.toBytes(endRowKey), true))
//    })
//
//    val filter = new MultiRowRangeFilter(ranges)
//    scan.setFilter(filter)
//    convertScanToString(scan)
//  }
//
//  def generateRowKey(pointMeasurement: String, date: Long): String = {
//    s"${pointMeasurement.reverse.padTo(8, '0').reverse}${date}00000"
//  }
//
//  def convertScanToString(scan: Scan): String = {
//    val proto = ProtobufUtil.toScan(scan)
//    Base64.getEncoder.encodeToString(proto.toByteArray)
//  }
//}
