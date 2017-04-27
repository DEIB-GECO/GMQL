package it.polimi.genomics.pythonapi

import java.util.concurrent.atomic.AtomicInteger

import it.polimi.genomics.GMQLServer.GmqlServer
import it.polimi.genomics.core.DataStructures.IRVariable
import it.polimi.genomics.core.ParsingType
import it.polimi.genomics.core.ParsingType.PARSING_TYPE
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.SparkContext
import it.polimi.genomics.pythonapi.operators.{ExpressionBuilder, OperatorManager}
import it.polimi.genomics.spark.implementation.loaders._

import scala.collection.JavaConversions._
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

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
    this.server = new GmqlServer(new GMQLSparkExecutor(sc=this.sparkContext, stopContext = false))
    this.logger.info("GMQL Server started")
  }

  def stopEngine(): Unit =
  {
    // get the spark context and kill it
    this.sparkContext.stop()
  }

  def getVariable(index : Int) : IRVariable = {
    this.variables.get(index).get
  }

  def putNewVariable(variable : IRVariable): Int =
  {
    val index = this.counter.getAndIncrement()
    this.variables(index) = variable
    index
  }

  def getServer : GmqlServer = {
    this.server
  }

  def getOperatorManager = {
    // generate a new operator manager
    val op = OperatorManager
    op
  }

  def getNewExpressionBuilder(index: Int) : ExpressionBuilder = {
    val expressionBuilder = new ExpressionBuilder(index)
    expressionBuilder
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

  def materialize(index : Int, outputPath : String): Unit =
  {
    // get the variable from the map
    val variableToMaterialize = this.variables.get(index)
    this.server setOutputPath outputPath MATERIALIZE variableToMaterialize.get
    //starting the server execution
    this.server.run()
    //clear the materialization list
    this.server.clearMaterializationList()
    //clear the cache of spark

  }

  def getParser(parserName: String) : BedParser =
  {
    parserName match {
      case "NarrowPeakParser" => NarrowPeakParser.asInstanceOf[BedParser]
      case "BasicParser" => BasicParser.asInstanceOf[BedParser]
      case _ => NarrowPeakParser.asInstanceOf[BedParser]
    }
  }

  def buildParser(delimiter: String, chrPos: Int, startPos: Int, stopPos: Int, strandPos: Int,
                  otherPos: java.util.List[java.util.List[String]]) : BedParser =
  {
    var strandPos_real :Option[Int] = None
    if(strandPos>=0) {
      strandPos_real = Option(strandPos)
    }
    var otherPosOptionList : Option[Array[(Int, ParsingType.PARSING_TYPE)]] = None
    val schemaList = new ListBuffer[(String, ParsingType.PARSING_TYPE)]()
    if(otherPos.size() > 0) {
      var otherPosList = Array[(Int, ParsingType.PARSING_TYPE)]()
      for(e <- otherPos) {
        val pos = e.get(0).toInt
        val name = e.get(1)
        val t = getParseTypeFromString(e.get(2))
        otherPosList = otherPosList :+ (pos, t)
        schemaList += Tuple2(name, t)
      }
      otherPosOptionList = Option(otherPosList)
    }

    val bedparser = new BedParser(delimiter,chrPos,startPos,stopPos,strandPos_real,otherPosOptionList)
    bedparser.schema = schemaList.toList
    bedparser
  }

  def getParseTypeFromString(typeString : String) = {
    ParsingType.attType(typeString)
  }


}
