package it.polimi.genomics.pythonapi

/**
  * Created by Luca Nanni on 11/04/17.
  * Email: luca.nanni@mail.polimi.it
  */
object AppProperties {
  var applicationName = "gmql_api"
  var serializer = "org.apache.spark.serializer.KryoSerializer"
  var master = "local[*]"
  var executorMemory = "6g"
  var driverMemory = "2g"
  var kryobuffer = "100m"

  /*
 * Utility functions to set the parameters of Spark
 * */
  def setApplicationName(applicationName : String): Unit =
  {
    this.applicationName = applicationName
  }

  def setSerializer(serializer : String): Unit =
  {
    this.serializer = serializer
  }

  def setMaster(master : String): Unit =
  {
    this.master = master
  }

}
