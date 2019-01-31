package it.polimi.genomics.federated

import com.softwaremill.sttp.{HttpURLConnectionBackend, sttp, _}
import it.polimi.genomics.GMQLServer.Implementation
import it.polimi.genomics.core.DAG._
import it.polimi.genomics.core.DataStructures.ExecutionParameters.BinningParameter
import it.polimi.genomics.core.DataStructures._
import it.polimi.genomics.core.{GMQLLoaderBase, GMQLSchema, GMQLSchemaCoordinateSystem, GMQLSchemaFormat}
import it.polimi.genomics.repository.federated.communication.{DownloadStatus, Failed, NotFound, Success}
import it.polimi.genomics.repository.federated.{GF_Communication, GF_Interface}
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import it.polimi.genomics.spark.implementation.loaders.CustomParser
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.collection.mutable


trait FederatedStep

case class Move(from: Instance, to: Instance) extends FederatedStep

case class LocalExecute(iRVariable: IRVariable) extends FederatedStep

case class RemoteExecute(iRVariable: IRVariable, instance: Instance) extends FederatedStep

class FederatedImplementation(val tempDir: Option[String] = None, val jobId: Option[String] = None) extends Implementation with Serializable {

  val api = GF_Communication.instance()

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
    if (dataset.contains(".")) {
      val schema: GMQLSchema = api.getSchema(dataset)
      val parser = new CustomParser

      parser.setSchema(schema)
      parser
    }
    else
      gse.getParser(name, dataset)
  }

  def call(irVars: List[IRVariable]) = {
    gse.to_be_materialized ++= irVars
    gse.go()
  }

  def moving(federatedJobId: String, dsName: String, from: String, to: Option[String]) = {
    if (to.isDefined) {
      implicit val backend = HttpURLConnectionBackend()

      val uri = uri"${to.get}federated/import/${federatedJobId}/${dsName}/${from}"
      println(uri)
      println
      println
      println

      val request = sttp
        .header("Accept", "application/xml")
        .header("X-AUTH-TOKEN", "FEDERATED-TOKEN")
        .get(uri)

      val response = request.send()

      println(response)
      println(response.body)
      println(response.statusText)
    } else {
      GF_Interface.instance().importDataset(federatedJobId, dsName, from)
    }


  }

  def poolingMoving(federatedJobId: String, dsName: String, to: Option[String]) = {
    if (to.isDefined) {


      implicit val backend = HttpURLConnectionBackend()

      var status = "started"
      val uri = uri"${to.get}federated/checkimport/${federatedJobId}/${dsName}"


      do {
        Thread.sleep(1000)


        val request = sttp
          .header("Accept", "application/xml")
          .header("X-AUTH-TOKEN", "FEDERATED-TOKEN")
          .get(uri)


        val response = request.send()

        println("  def poolingMoving(federatedJobId: String, dsName: String, to: String) = {")

        println(response)
        println(response.body)
        println(response.statusText)


        if (response.code != 200) {
          throw new GmqlFederatedException("Fatal federated error: trace response code != 200 ")
        }

        status = (scala.xml.XML.loadString(response.unsafeBody) \\ "status").text

        if (status.toLowerCase.equals("failed") || status.toLowerCase.equals("notfound"))
          throw new GmqlFederatedException("Fatal federated error: trace-> " + status)

        println(s"\t\t\t\t\t${federatedJobId}/${dsName}")

      } while (!status.toLowerCase.equals("success"))
    } else {
      var status: DownloadStatus = null
      do {
        Thread.sleep(1000)

        status = GF_Interface.instance().checkImportStatus(federatedJobId, dsName)
        if (status.isInstanceOf[Failed] || status.isInstanceOf[NotFound])
          throw new GmqlFederatedException("Fatal federated error: trace-> " + status)

      } while (!status.isInstanceOf[Success])


    }
  }


  def pooling(remoteServerUri: String, remoteJobId: String) = {
    implicit val backend = HttpURLConnectionBackend()

    var status = "started"

    do {
      Thread.sleep(1000)

      val uri = uri"${remoteServerUri}jobs/$remoteJobId/trace"


      val request = sttp
        .header("Accept", "application/xml")
        .header("X-AUTH-TOKEN", "FEDERATED-TOKEN")
        .get(uri)


      val response = request.send()

      if (response.code != 200) {
        throw new GmqlFederatedException("Fatal federated error: trace response code != 200 ")
      }

      status = (scala.xml.XML.loadString(response.body.right.get) \\ "status").text

      if (status.contains("FAILED") || status.contains("STOPPED"))
        throw new GmqlFederatedException("Fatal federated error: trace-> " + status)

      println("\t\t\t\t\t" + remoteJobId + status)

    } while (!status.equals("EXEC_SUCCESS"))
  }

  def callRemote(irVars: List[IRVariable], instance: GMQLInstance) = {
    implicit val backend = HttpURLConnectionBackend()

    val remoteServerUri = GF_Communication.instance().getLocation(instance.name).URI


    val serilizedDag = DAGSerializer.serializeDAG(DAGWrapper(irVars))


    val uri = uri"${remoteServerUri}queries/dag/tab?federatedJobId=$jobId"


    val request = sttp.body(serilizedDag)
      .header("Accept", "application/xml")
      .header("X-AUTH-TOKEN", "FEDERATED-TOKEN")
      .post(uri)


    val response = request.send()

    if (response.code != 200) {
      throw new Exception("Fatal federated error response code  != 200 ")
    }


    val remoteJobId = (scala.xml.XML.loadString(response.body.right.get) \\ "id").text

    //add waiting execution
    pooling(remoteServerUri, remoteJobId)


    //add move
    val remoteDsName =
      irVars.head.regionDag match {
        case federated: Federated => federated.name
        case _ => irVars.head.metaDag.asInstanceOf[Federated].name
      }


    val fedIrVars: List[IROperator] = irVars.flatMap(irVar =>
      irVar.metaDag.getDependencies ++ irVar.regionDag.getDependencies
    ).filter(_.isInstanceOf[Federated])


    println("\t\t\t\t\t\t\t" + fedIrVars)


    println(response.body)

  }

  val previouslyRunDag = mutable.Set.empty[ExecutionDAG]

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





    //    val remoteServerUri = "http://localhost:8000/gmql-rest" //check slash

    //    println(executionDag.getFederatedsources)
    //    executionDag.getFederatedsources.foreach {
    //      case (dsName) =>
    //        println(s"PRE MOVING ${jobId.get}, $dsName, ${instance.name}, $remoteServerUri")
    //        moving(jobId.get, dsName, instance.name, remoteServerUri)
    //        println(s"AFTER MOVING ${jobId.get}, $dsName, ${instance.name}, $remoteServerUri")
    //
    //        poolingMoving(jobId.get, dsName, remoteServerUri)
    //        println(s"AFTER poolingMoving ${jobId.get}, $dsName, ${instance.name}, $remoteServerUri")
    //    }

    val irVarFiltered = irVars.filter { x =>
      !(x.regionDag.isInstanceOf[IRNoopRD] && x.metaDag.isInstanceOf[IRStoreMD])
    }


    if (!previouslyRunDag.contains(executionDag)) {
      //send other
      whereExDag match {
        case LOCAL_INSTANCE =>
          println("call(irVarFiltered)")
          call(irVarFiltered)
        case _: GMQLInstance =>

          println("callRemote(irVarFiltered)")
          //TODO
          //        call(irVarFiltered)
          callRemote(irVarFiltered, whereExDag)
        //          println("MOVE DATA from " + whereExDag + " to " + destination)
      }
      previouslyRunDag.add(executionDag)
    }

    val dssNamesToCopy = executionDag.dag.flatMap(_.roots).filter(_.isInstanceOf[Federated]).map(_.asInstanceOf[Federated].name)

    val remoteServerUriOpt =
      if (destination != LOCAL_INSTANCE)
        Some(GF_Communication.instance().getLocation(destination.name).URI)
      else
        None

    dssNamesToCopy.foreach { dsName =>
      println("MOVE DATA(" + dsName + ") from " + whereExDag + " to " + destination)
      //      if(remoteServerUriOpt.isDefined) {
      moving(jobId.get, dsName, whereExDag.name, remoteServerUriOpt)
      poolingMoving(jobId.get, dsName, remoteServerUriOpt)

      //      }
      //      else{/**/
      //        GF_Interface.instance().importDataset(jobId.get,dsName,whereExDag.name)
      //      }


    }


  }

  def implementation(): Unit = {
    val opDAG = new OperatorDAG(to_be_materialized.flatMap(x => List(x.metaDag, x.regionDag)).toList)

    val opDAGFrame = new OperatorDAGFrame(opDAG)
    showFrame(opDAGFrame, "OperatorDag")

    //TODO check .get
    val dagSplits = DAGManipulator.splitDAG(opDAG, jobId.get, tempDir.get)
    val executionDAGs = DAGManipulator.generateExecutionDAGs(dagSplits.values.toList)

    val f2 = new MetaDAGFrame(executionDAGs)
    showFrame(f2, "ExDag")


    executionDAGs.roots.foreach(recursiveCall(_, LOCAL_INSTANCE))
  }


}


