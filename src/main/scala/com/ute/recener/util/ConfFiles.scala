package com.ute.recener.util

import java.util.Properties
import scala.io.Source

object ConfFiles {

  final val spark_properties = "spark.properties"
  val url = getClass.getClassLoader.getResource(spark_properties)
  val properties: Properties = new Properties()

  def getConfiguration(): Properties = {

    if (url != null) {
      val source = Source.fromURL(url)
      properties.load(source.bufferedReader())
    }
    properties
  }

  def getHbaseConfiguration(): String = {

    val prop: Properties = getConfiguration()

    val zookeeperQuorum = prop.getProperty("zookeeper.quorum")
    val zookeeperclientport = prop.getProperty("zookeeper.clientport")

    s"""{
        "hbase.zookeeper.quorum":"$zookeeperQuorum",
        "hbase.zookeeper.property.clientPort":"$zookeeperclientport"
      }
      """

  }

  def getApplicationMode(): String = {
    val prop: Properties = getConfiguration();
    prop.getProperty("application.mode")
  }

}
