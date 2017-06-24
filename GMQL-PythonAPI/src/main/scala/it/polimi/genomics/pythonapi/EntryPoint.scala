package it.polimi.genomics.pythonapi

import java.io.{File, PrintWriter}
import java.util
import java.util.Collections

import org.apache.log4j.varia.NullAppender
import org.apache.log4j.{LogManager, Logger}
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
  * */
object EntryPoint {

  val logger = LoggerFactory.getLogger(this.getClass)
  val properties = AppProperties
  def main(args: Array[String]): Unit = {

    // SILENCE EVERYTHING
    Logger.getRootLogger.removeAllAppenders()
    Logger.getRootLogger.addAppender( new NullAppender)

    val port = args(0).toInt

    //val sc = startSparkContext()

    //val pythonManager = PythonManager
    //pythonManager.setSparkContext(sc=sc)

    val gatewayServer : GatewayServer = new GatewayServer(this, port)
    gatewayServer.start()
    this.logger.info("GatewayServer started")

    /*Synchronization with the python process*/
    val pw = new PrintWriter(new File("sync.txt"))
    pw.write("Spark context started")
    pw.close()
  }

  def startSparkContext(): SparkContext =
  {
    /*
    * Setting up the Spark context
    * */
    val conf = new SparkConf()
      .setAppName(properties.applicationName)
      .setMaster(properties.master)
      .set("spark.serializer", properties.serializer)
      .set("spark.executor.memory", properties.executorMemory)
      .set("spark.driver.memory", properties.driverMemory)
      .set("spark.kryoserializer.buffer.max", properties.kryobuffer)

    val sc = SparkContext.getOrCreate(conf)
    logger.info("Spark Context initiated")
    sc
  }
}
