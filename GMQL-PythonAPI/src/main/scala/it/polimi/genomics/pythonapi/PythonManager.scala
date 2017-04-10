package it.polimi.genomics.pythonapi

import java.util.concurrent.atomic.AtomicInteger

import it.polimi.genomics.GMQLServer.GmqlServer
import it.polimi.genomics.core.DataStructures.IRVariable
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.SparkContext
import it.polimi.genomics.pythonapi.operators.{Operator, SelectOperator}
import it.polimi.genomics.spark.implementation.loaders.{BedParser, NarrowPeakParser}
import org.slf4j.LoggerFactory

/**
  * Created by Luca Nanni on 08/04/17.
  * Email: luca.nanni@mail.polimi.it
  */

/**
  * Main entry point for the external API. It instantiate the server, create
  * the spark context and the executor.
  */
object PythonManager {

  private val logger = LoggerFactory.getLogger(this.getClass)

  /*
  * This data structure stores in memory the variables used by the programmer.
  * The variables are allocated with a counter (Int)
  * */
  private val variables = collection.mutable.Map[Int, IRVariable]()
  /*
  * Thread-safe counter for storing the ids of the variables in the
  * scala map above
  * */
  private val counter: AtomicInteger = new AtomicInteger(0)
  private var server : GmqlServer = _
  private var sparkContext : SparkContext = _

  def setSparkContext(sc : SparkContext): Unit =
  {
    this.sparkContext = sc
  }

  def startEngine(): Unit =
  {
    // set the server and the executor
    this.server = new GmqlServer(new GMQLSparkExecutor(sc=this.sparkContext))
    this.logger.info("GMQL Server started")
  }

  def stopEngine(): Unit =
  {
    // get the spark context and kill it
    this.sparkContext.stop()
  }

  def getOperator(operatorName: String): Operator =
  {
    operatorName match {
      case "select" => SelectOperator
    }
  }

  def read_dataset(dataset_path: String, parserName: String): Int =
  {
    val parser : BedParser = this.getParser(parserName = parserName)
    val dataset : IRVariable = this.server READ dataset_path USING parser
    //putting the new variable in the map
    val index = this.counter.getAndIncrement()
    this.variables(index) = dataset
    //return the index of the variable to the caller
    index
  }

  def getParser(parserName: String) : BedParser =
  {
    parserName match {
      case "NarrowPeakParser" => NarrowPeakParser.asInstanceOf[BedParser]
      case _ => NarrowPeakParser.asInstanceOf[BedParser]
    }
  }
}
