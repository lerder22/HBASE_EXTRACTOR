package com.ute.recener.util


import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes

object MappingSchema extends java.io.Serializable {
  private val serialVersionUID = 1L

  ///////////////////////////////////////////////
  /////////////// OPTIMAL MEASURE ///////////////
  ///////////////////////////////////////////////

  case class OptimalMeasure(rowkey: String, day: Long, magnitude: Long, measurement_interval: Long, measuring_point: Long, period: Int,
                            value: Double, validation_result: String, source: Long, last_update_time: Long)

    def parseOmRow(result: Result,cfDataBytesOM:String): OptimalMeasure = {
      val rowkey = Bytes.toString(result.getRow())
      // remove time from rowKey, stats row key is for day
      val p0 = rowkey.split(" ")(0)
      val p1 = Bytes.toLong(result.getValue(Bytes.toBytes(cfDataBytesOM), Bytes.toBytes("D")))
      val p2 = Bytes.toLong(result.getValue(Bytes.toBytes(cfDataBytesOM), Bytes.toBytes("M")))
      val p3 = Bytes.toLong(result.getValue(Bytes.toBytes(cfDataBytesOM), Bytes.toBytes("MI")))
      val p4 = Bytes.toLong(result.getValue(Bytes.toBytes(cfDataBytesOM), Bytes.toBytes("MP")))
      val p5 = Bytes.toInt(result.getValue(Bytes.toBytes(cfDataBytesOM), Bytes.toBytes("P")))
      val p6 = Bytes.toDouble(result.getValue(Bytes.toBytes(cfDataBytesOM), Bytes.toBytes("V")))
      val p7 = Bytes.toString(result.getValue(Bytes.toBytes(cfDataBytesOM), Bytes.toBytes("VR")))
      val p8 = Bytes.toLong(result.getValue(Bytes.toBytes(cfDataBytesOM), Bytes.toBytes("S")))
      val p9 = Bytes.toLong(result.getValue(Bytes.toBytes(cfDataBytesOM), Bytes.toBytes("L")))
      OptimalMeasure(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9)
    }


  ////////////////////////////////////////////////////
  /////////////// INSTANTANEOUS VALUE ///////////////
  ////////////////////////////////////////////////////

  case class InsValue(rowkey: String, magnitude: Long, measurement_interval: Long, source: Long, measuring_point: Long, value: Double,
                      date: Long, last_update_time: Long, validation_result: String)


  def parseIvRow(result: Result, cfData: String): InsValue = {

    val rowkey = Bytes.toString(result.getRow())
    // remove time from rowKey, stats row key is for day
    val p0 = rowkey.split(" ")(0)

    // Función auxiliar para obtener valores Long de HBase, devuelve 0 si es null
    def getLongValue(cf: String, qualifier: String): Long = {
      val value = result.getValue(Bytes.toBytes(cf), Bytes.toBytes(qualifier))
      if (value != null) Bytes.toLong(value) else -1L
    }

    // Función auxiliar para obtener valores Double de HBase, devuelve 0.0 si es null
    def getDoubleValue(cf: String, qualifier: String): Double = {
      val value = result.getValue(Bytes.toBytes(cf), Bytes.toBytes(qualifier))
      if (value != null) Bytes.toDouble(value) else -1.0
    }

    val p1 = getLongValue(cfData, "M")
    val p2 = getLongValue(cfData, "MI")
    val p3 = getLongValue(cfData, "S")
    val p4 = getLongValue(cfData, "MP")
    val p5 = getDoubleValue(cfData, "V")
    val p6 = getLongValue(cfData, "D")
    val p7 = getLongValue(cfData, "L")

    // Para valores String, si el resultado es null, podemos devolver un String vacío
    val p8 = Option(Bytes.toString(result.getValue(Bytes.toBytes(cfData), Bytes.toBytes("VR")))).getOrElse("")

    InsValue(p0, p1, p2, p3, p4, p5, p6, p7, p8)
  }


  ////////////////////////////////////////////////
  /////////////// EVENT //////////////////////////
  ////////////////////////////////////////////////

  case class Event(rowkey: String, day: Long, file: String, information: String, last_update_time: Long, measuring_point:Long,
                   source: Long, event_type: Long)

  def parseERow(result: Result, cfData: String): Event = {

    val rowkey = Bytes.toString(result.getRow())
    // remove time from rowKey, stats row key is for day
    val p0 = rowkey.split(" ")(0)

    val p1 = Bytes.toLong(result.getValue(Bytes.toBytes(cfData), Bytes.toBytes("D")))
    val p2 = Bytes.toString(result.getValue(Bytes.toBytes(cfData), Bytes.toBytes("F")))
    val p3 = Bytes.toString(result.getValue(Bytes.toBytes(cfData), Bytes.toBytes("I")))
    val p4 = Bytes.toLong(result.getValue(Bytes.toBytes(cfData), Bytes.toBytes("L")))
    val p5 = Bytes.toLong(result.getValue(Bytes.toBytes(cfData), Bytes.toBytes("MP")))
    val p6 = Bytes.toLong(result.getValue(Bytes.toBytes(cfData), Bytes.toBytes("S")))
    val p7 = Bytes.toLong(result.getValue(Bytes.toBytes(cfData), Bytes.toBytes("T")))

    Event(p0, p1, p2, p3, p4, p5, p6, p7)

  }

}
