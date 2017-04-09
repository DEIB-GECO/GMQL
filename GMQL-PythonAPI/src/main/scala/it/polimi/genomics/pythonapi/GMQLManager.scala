package it.polimi.genomics.pythonapi

import it.polimi.genomics.GMQLServer.GmqlServer
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.{SparkConf, SparkContext}
import it.polimi.genomics.pythonapi.operators.{Operator, SelectOperator}

/**
  * Created by Luca Nanni on 08/04/17.
  * Email: luca.nanni@mail.polimi.it
  */

/**
  * Main entry point for the external API. It instantiate the server, create
  * the spark context and the executor.
  */
object GMQLManager {

  private var applicationName = "gmql_api"
  private var serializer = "org.apache.spark.serializer.KryoSerializer"
  private var master = "local[*]"

  private var server : GmqlServer = _

  // Utility functions to set the parameters of Spark
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

  def startEngine(): Unit =
  {
    val conf = new SparkConf()
                  .setAppName(this.applicationName)
                  .setMaster(this.master)
                  .set("spark.serializer", this.serializer)

    val sc = new SparkContext(conf)

    // set the server and the executor
    this.server = new GmqlServer(new GMQLSparkExecutor(sc=sc))
  }

  def getOperator(operatorName: String): Operator =
  {
    operatorName match {
      case "select" => SelectOperator
    }
  }
}
