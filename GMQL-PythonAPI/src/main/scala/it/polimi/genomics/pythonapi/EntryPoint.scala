package it.polimi.genomics.pythonapi

import java.io.{File, PrintWriter}

import org.apache.log4j.Logger
import org.apache.log4j.varia.NullAppender
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import py4j.GatewayServer

/**
  * Created by Luca Nanni on 08/04/17.
  * Email: luca.nanni@mail.polimi.it
  */

/**
  * Main of the application. It instantiates a gateway server for Python
  * to access to the JVM (through py4j)
  **/
object EntryPoint {

  val logger = LoggerFactory.getLogger(this.getClass)
  val properties = AppProperties

  def main(args: Array[String]): Unit = {

    // SILENCE EVERYTHING
    Logger.getRootLogger.removeAllAppenders()
    Logger.getRootLogger.addAppender(new NullAppender)

    val port = args(0).toInt

    //val sc = startSparkContext()

    //val pythonManager = PythonManager
    //pythonManager.setSparkContext(sc=sc)

    val gatewayServer: GatewayServer = new GatewayServer(this, port)
    gatewayServer.start()
    this.logger.info("GatewayServer started")

    /*Synchronization with the python process*/
    val pw = new PrintWriter(new File("sync.txt"))
    pw.write("Spark context started")
    pw.close()
  }
}
