package com.ute.recener.util

import java.util.Properties

object DataBaseConnection {

  final val database_properties = "database.properties"
  val resourceAsStream = getClass().getClassLoader().getResourceAsStream(database_properties)
  var props = new Properties()
  var jdbcUrl: String = null
  var jdbcHiveUrl: String = null
  var connectionProperties: Properties = null
  var connectionPropertiesHive: Properties = null

  def loadPropertiesFile() {

    val resourceAsStream = getClass().getClassLoader().getResourceAsStream(database_properties)
    props.load(resourceAsStream)
    jdbcUrl = props.getProperty("database.url")
    connectionProperties = new Properties()
    connectionProperties.put("user", props.getProperty("database.username"))
    connectionProperties.put("password", props.getProperty("database.password"))
    connectionProperties.put("driver", props.getProperty("database.driverClassName"))

  }

  def loadPropertiesHiveFile() {

    val resourceAsStream = getClass().getClassLoader().getResourceAsStream(database_properties)
    props.load(resourceAsStream)
    jdbcHiveUrl = props.getProperty("bigdata.hive.url")
    connectionPropertiesHive = new Properties()
    connectionPropertiesHive.put("user", props.getProperty("bigdata.hive.user"))
    connectionPropertiesHive.put("password", props.getProperty("bigdata.hive.password"))
    connectionPropertiesHive.put("driver", props.getProperty("bigdata.hive.driver"))

  }

  def getUrl(): String = {
    if (jdbcUrl == null) {
      loadPropertiesFile()
    }
    jdbcUrl
  }

  def getHiveUrl(): String = {
    if (jdbcHiveUrl == null) {
      loadPropertiesHiveFile()
    }
    jdbcHiveUrl
  }

  def getConnectionPropertiesHive: Properties = {
    if (connectionPropertiesHive == null) {
      loadPropertiesHiveFile()
    }
    connectionPropertiesHive
  }

  def getConnectionProperties: Properties = {
    if (connectionProperties == null) {
      loadPropertiesFile()
    }
    connectionProperties
  }


}
