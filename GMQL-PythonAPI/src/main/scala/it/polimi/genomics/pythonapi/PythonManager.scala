package it.polimi.genomics.pythonapi

import java.io.{File, FileFilter}
import java.util.concurrent.atomic.AtomicInteger

import it.polimi.genomics.GMQLServer.GmqlServer
import it.polimi.genomics.core.DataStructures._
import it.polimi.genomics.core.ParsingType.PARSING_TYPE
import it.polimi.genomics.core._
import it.polimi.genomics.pythonapi.operators.{ExpressionBuilder, OperatorManager}
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import it.polimi.genomics.spark.implementation.loaders._
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
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

  val logger = LoggerFactory.getLogger(this.getClass)

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
  private var server: GmqlServer = _
  private var sparkContext: Option[SparkContext] = None
  private var sparkConfigs: Option[SparkConf] = None

  /*
  * GMQLServer related stuff
  * */

  def setSparkConfiguration(sc: SparkConf): Unit = {
    this.sparkConfigs = Some(sc)
  }

  def setSparkContext(sc: SparkContext): Unit = {
    this.sparkContext = Some(sc)
  }

  def startEngine(): Unit = {
    // set the server and the executor
    logger.info("Setting the GMQL Server")
    this.server = new GmqlServer(new StubExecutor())
  }

  def stopEngine(): Unit = {
    // get the spark context and kill it
    if (this.sparkContext.isDefined)
      this.sparkContext.get.stop()
  }

  def shutdown(): Unit = {
    logger.info("Shutting down the backend")
    this.stopEngine()
    System.exit(0)
  }


  /*
  * VARIABLES
  * */
  def getVariable(index: Int): IRVariable = {
    logger.info(s"Getting variable $index")
    this.variables(index)
  }

  def putNewVariable(variable: IRVariable): Int = {
    val index = this.counter.getAndIncrement()
    logger.info(s"Putting new variable $index")
    this.variables(index) = variable
    index
  }

  def cloneVariable(index: Int): Int = {
    logger.info(s"Cloning variable $index")
    val variable = this.getVariable(index)
    val new_variable = DAGSerializer.deserializeDAG(DAGSerializer.serializeDAG(DAGWrapper(List(variable)))).dag.head
    this.putNewVariable(new_variable)
  }

  def getVariableSchemaNames(index: Int): java.util.List[String] = {
    logger.info(s"Getting schema of variable $index")
    val variable = this.getVariable(index)
    val result = variable.schema.map({ case (a, b) => a })
    result.asJava
  }

  /*
  * MANAGEMENT
  * */
  def getServer: GmqlServer = {
    this.server
  }

  def getOperatorManager = {
    // generate a new operator manager
    logger.info(s"Getting OperatorManager")
    val op = OperatorManager
    op
  }

  def getNewExpressionBuilder(index: Int): ExpressionBuilder = {
    logger.info(s"Getting ExpressionBuilder")
    val expressionBuilder = new ExpressionBuilder(index)
    expressionBuilder
  }

  /*
  * Parsing of datasets
  * */

  @deprecated
  def read_dataset(dataset_path: String): Int = {
    val parser: CustomParser = new CustomParser()
    parser.setSchema(findSchemaFile(dataset_path))
    read_dataset(dataset_path, parser)
  }

  @deprecated
  def findSchemaFile(dataset_path: String): String = {
    val dir = new File(dataset_path)
    if (dir.isDirectory) {
      val files = dir.listFiles(new FileFilter {
        override def accept(pathname: File): Boolean = {
          pathname.getPath.endsWith(".schema")
        }
      })
      if (files.size > 1)
        throw new IllegalStateException("There is more than one schema file in the directory")
      files(0).getAbsolutePath //take the first schema file
    }
    else
      throw new IllegalArgumentException("The dataset path must be a directory!")
  }

  def read_dataset(dataset_path: String, parserName: String): Int = {
    val parser: BedParser = this.getParser(parserName = parserName)
    read_dataset(dataset_path, parser)
  }

  def read_dataset(dataset_path: String, parser: BedParser): Int = {
    val dataset: IRVariable = this.server READ dataset_path USING parser
    //putting the new variable in the map
    val index = this.counter.getAndIncrement()
    this.variables(index) = dataset
    //return the index of the variable to the caller
    index
  }

  def getParser(parserName: String): BedParser = {
    parserName.toLowerCase match {
      case "narrowpeakparser" => NarrowPeakParser.asInstanceOf[BedParser]
      case "basicparser" => BasicParser.asInstanceOf[BedParser]
      case _ => NarrowPeakParser.asInstanceOf[BedParser]
    }
  }

  def buildParser(delimiter: String, chrPos: Int, startPos: Int, stopPos: Int,
                  strandPos: Option[Int], otherPos: Option[java.util.List[java.util.List[String]]],
                  parsingType: String, coordinateSystem: String) = {

    val pType = GMQLSchemaFormat.getType(parsingType)
    val cSystem = GMQLSchemaCoordinateSystem.getType(coordinateSystem)
    var schema: List[(String, PARSING_TYPE)] = List()
    var convertedOtherPos: Option[Array[(Int, ParsingType.PARSING_TYPE)]] = None
    if (otherPos.isDefined) {
      val otherPosAll: Array[(Int, String, ParsingType.PARSING_TYPE)] = {
        otherPos.get.map(x => {
          val xAsList = x.asScala.toList
          val pos = xAsList.get(0).toInt // get the position
          val name = xAsList.get(1) // get the name
          val t = getParseTypeFromString(xAsList.get(2)) // get the PARSING_TYPE
          (pos, name, t)
        }).toArray
      }

      schema = otherPosAll.sortBy(x => x._1).map(x => (x._2, x._3)).toList
      convertedOtherPos = Some(otherPosAll.sortBy(x => x._1).map(x => (x._1, x._3)))
    }

    val res = new BedParser(delimiter, chrPos, startPos, stopPos, strandPos, convertedOtherPos)
    res.schema = schema
    res.coordinateSystem = cSystem
    res.parsingType = pType
    res
  }

  @deprecated
  def buildParser_old(delimiter: String, chrPos: Int, startPos: Int, stopPos: Int, strandPos: Int,
                      otherPos: java.util.List[java.util.List[String]]): BedParser = {
    var strandPos_real: Option[Int] = None
    if (strandPos >= 0) {
      strandPos_real = Option(strandPos)
    }
    var otherPosOptionList: Option[Array[(Int, ParsingType.PARSING_TYPE)]] = None
    val schemaList = new ListBuffer[(String, ParsingType.PARSING_TYPE)]()
    if (otherPos.size() > 0) {
      var otherPosList = Array[(Int, ParsingType.PARSING_TYPE)]()
      for (e <- otherPos) {
        val pos = e.get(0).toInt
        val name = e.get(1)
        val t = getParseTypeFromString(e.get(2))
        otherPosList = otherPosList :+ (pos, t)
        schemaList += Tuple2(name, t)
      }
      otherPosOptionList = Option(otherPosList)
    }

    val bedparser = new BedParser(delimiter, chrPos, startPos, stopPos, strandPos_real, otherPosOptionList)
    bedparser.schema = schemaList.toList
    bedparser
  }


  /*
  * TYPES
  * */

  def getParseTypeFromString(typeString: String): PARSING_TYPE = {
    ParsingType.attType(typeString)
  }

  def getStringFromParsingType(parsingType: PARSING_TYPE): String = {
    parsingType match {
      case ParsingType.DOUBLE => "double"
      case ParsingType.INTEGER => "int"
      case ParsingType.LONG => "long"
      case ParsingType.CHAR => "char"
      case ParsingType.STRING => "string"
      case ParsingType.NULL => "null"
      case _ => "string"
    }
  }

  def getNone = None

  def getSome(thing: Any) = Some(thing)

  /*
  * Materialization
  * */

  def materialize(index: Int, outputPath: String): Unit = {
    logger.info(s"Materializing variable $index at $outputPath")
    // get the variable from the map
    val variableToMaterialize = this.variables.get(index)
    this.server setOutputPath outputPath MATERIALIZE variableToMaterialize.get
  }

  def get_serialized_materialization_list(): String = {
    val materializationList = this.server.materializationList.toList
    DAGSerializer.serializeDAG(DAGWrapper(materializationList))
  }

  def execute(): Unit = {
    this.checkSparkContext()
    //starting the server execution
    this.server.run()
    //clear the materialization list
    this.server.clearMaterializationList()
  }

  def collect(index: Int): CollectedResult = {
    this.checkSparkContext()

    val variableToCollect = this.variables.get(index)
    val result = this.server COLLECT_ITERATOR variableToCollect.get
    // this.stopSparkContext()
    new CollectedResult(result)
  }

//  def take(index: Int, n: Int): CollectedResult = {
//    this.checkSparkContext()
//    val variableToTake = this.getVariable(index)
//    val result = this.server.TAKE(variableToTake, n)
//    new CollectedResult(result)
//  }

  def serializeVariable(index: Int): String = {
    val variableToSerialize = this.getVariable(index)
    DAGSerializer.serializeDAG(DAGWrapper(List(variableToSerialize)))
  }

  def modify_dag_source(index: Int, source: String, dest: String): Unit = {
    val variable = this.getVariable(index)
    modify_dag_source(variable.metaDag, source, dest)
    modify_dag_source(variable.regionDag, source, dest)
  }

  def modify_dag_source(dag: IROperator, source: String, dest: String): Unit = {
    dag match {
      case x: IRReadMD[_, _, _, _] =>
        if (x.dataset.position == source) {
          val newDataset = IRDataSet(dest, x.dataset.schema)
          x.dataset = newDataset
          x.paths = List(dest)
        }
      case x: IRReadRD[_, _, _, _] =>
        if (x.dataset.position == source) {
          val newDataset = IRDataSet(dest, x.dataset.schema)
          x.dataset = newDataset
          x.paths = List(dest)
        }
      case _ =>
    }
    dag.getOperatorList.map(operator => modify_dag_source(operator, source, dest))
  }

  /*Spark context related*/

  def checkSparkContext(): Unit = {
    /*Check if there is an instantiated spark context*/
    if (this.sparkContext.isEmpty) {
      val sc = this.startSparkContext()
      this.server.implementation = new GMQLSparkExecutor(sc = sc, stopContext = false, profileData = false)
      this.setSparkContext(sc)
    }
  }

  def startSparkContext(): SparkContext = {
    if (this.sparkConfigs.isDefined) SparkContext.getOrCreate(this.sparkConfigs.get)
    else SparkContext.getOrCreate()
  }

  def stopSparkContext(): Unit = {
    this.sparkContext.get.stop()
    this.sparkContext = None
  }

  def setSparkLocalDir(dir: String): Unit = {
    EntryPoint.properties.sparkLocalDir = dir
  }

  def setHadoopHomeDir(dir: String): Unit = {
    System.setProperty("hadoop.home.dir", dir)
  }

  def setSparkConfiguration(appName: String, master: String,
                            conf: java.util.Map[String, String]): Unit = {
    this.sparkConfigs = Some(
      new SparkConf()
      .setAppName(appName)
      .setMaster(master)
      .setAll(conf)
    )
  }

  def setSystemConfiguration(conf: java.util.Map[String, String]): Unit = {
    val properties = System.getProperties
    properties.putAll(conf)
    System.setProperties(properties)
  }
}
