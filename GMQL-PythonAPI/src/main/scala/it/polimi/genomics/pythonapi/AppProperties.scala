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
  var driverMemory = "8g"
  var kryobuffer = "1g"
  var maxResultSize = "5g"
  var driverHost = "localhost"

}
