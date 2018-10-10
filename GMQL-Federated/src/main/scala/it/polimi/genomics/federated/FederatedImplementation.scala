package it.polimi.genomics.federated

import com.softwaremill.sttp.{HttpURLConnectionBackend, sttp, _}
import it.polimi.genomics.GMQLServer.Implementation
import it.polimi.genomics.core.DAG._
import it.polimi.genomics.core.DataStructures.ExecutionParameters.BinningParameter
import it.polimi.genomics.core.DataStructures._
import it.polimi.genomics.core.{GMQLLoaderBase, GMQLSchemaCoordinateSystem, GMQLSchemaFormat}
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory


trait FederatedStep

case class Move(from: Instance, to: Instance) extends FederatedStep

case class LocalExecute(iRVariable: IRVariable) extends FederatedStep

case class RemoteExecute(iRVariable: IRVariable, instance: Instance) extends FederatedStep

class FederatedImplementation(val tempDir: String, val jobId: String) extends Implementation with Serializable {

  def showFrame[T <: DAGNode[T]](dagFrame: DAGFrame[T], title: String): Unit = {
    dagFrame.setSize(1000, 600)
    dagFrame.setVisible(true)
    dagFrame.setTitle(title)
  }

  final val logger = LoggerFactory.getLogger(this.getClass)


  val binningPar = BinningParameter(Some(1000))


  val conf = new SparkConf().setAppName("GMQL V2.1 Spark ")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.kryoserializer.buffer", "128")
    .set("spark.driver.allowMultipleContexts", "true")
    .set("spark.sql.tungsten.enabled", "true").setMaster("local[*]")
  val sc: SparkContext = SparkContext.getOrCreate(conf)

  val gse = new GMQLSparkExecutor(
    testingIOFormats = false,
    sc = sc,
    outputFormat = GMQLSchemaFormat.TAB,
    outputCoordinateSystem = GMQLSchemaCoordinateSystem.Default,
    stopContext = false)


  /** Starts the execution */
  override def go(): Unit = {
    println("####  Federated #####")
    implementation()
  }

  override def collect(iRVariable: IRVariable): Any = throw new NotImplementedError()

  override def take(iRVariable: IRVariable, n: Int): Any = throw new NotImplementedError()

  /** stop GMQL implementation (kill a job) */
  override def stop(): Unit = println("stop")

  /** given the name of a parser, returns it. It must at least provide a parser for the "default" name */
  override def getParser(name: String, dataset: String): GMQLLoaderBase = {
    println("getParser")
    gse.getParser(name, dataset)
  }

  def call(irVars: List[IRVariable]) = {
    gse.to_be_materialized ++= irVars
    gse.go()
  }

  def callRemote(irVars: List[IRVariable], instance: GMQLInstance) = {
    val serilizedDag = DAGSerializer.serializeDAG(DAGWrapper(irVars))
    //TODO change
    // send job_id with an extension _1, _2
    val uri = uri"http://localhost:8000/gmql-rest/queries/dag/tab?federatedJobId=$jobId"


    val request = sttp.body(serilizedDag)
      .header("Accept", "application/xml")
      .header("X-AUTH-TOKEN", "FEDERATED-TOKEN")
      .post(uri)


    implicit val backend = HttpURLConnectionBackend()
    val response = request.send()


    Thread.sleep(1000)
    println(response.body)

  }


  def recursiveCall(executionDag: ExecutionDAG, destination: GMQLInstance): Unit = {
    val whereExDag: GMQLInstance = executionDag.where

    if (executionDag.hasDependencies)
      executionDag.getDependencies.foreach(t => recursiveCall(t, whereExDag))
    val irVars: List[IRVariable] = try {
      executionDag.toIRVariable(binningPar)
    } catch {
      case _: IllegalStateException =>
        executionDag.dag.flatMap { x =>
          x.roots.map {
            case x: RegionOperator => IRVariable(IRNoopMD(), x)(binningPar)
            case x: MetaOperator => IRVariable(x, IRNoopRD())(binningPar)
          }
        }
    }

    val irVarFiltered = irVars.filter { x =>
      !(x.regionDag.isInstanceOf[IRNoopRD] && x.metaDag.isInstanceOf[IRStoreMD])
    }

    //send other
    whereExDag match {
      case LOCAL_INSTANCE =>
        println("call(irVarFiltered)")
        call(irVarFiltered)
      case _: GMQLInstance =>
        println("SEND " + executionDag + " to " + whereExDag)
        println("callRemote(irVarFiltered)")
        //TODO
        //        call(irVarFiltered)
        callRemote(irVarFiltered, whereExDag)
        println("SEND DATA from " + whereExDag + " to " + destination)
    }


  }

  def implementation(): Unit = {
    val opDAG = new OperatorDAG(to_be_materialized.flatMap(x => List(x.metaDag, x.regionDag)).toList)

    val opDAGFrame = new OperatorDAGFrame(opDAG)
    showFrame(opDAGFrame, "OperatorDag")


    val dagSplits = DAGManipulator.splitDAG(opDAG, jobId, tempDir)
    val executionDAGs = DAGManipulator.generateExecutionDAGs(dagSplits.values.toList)

    val f2 = new MetaDAGFrame(executionDAGs)
    showFrame(f2, "ExDag")


    executionDAGs.roots.foreach(recursiveCall(_, LOCAL_INSTANCE))
  }


}


