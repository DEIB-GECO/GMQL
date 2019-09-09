package it.polimi.genomics.federated

import com.softwaremill.sttp.{HttpURLConnectionBackend, sttp, _}
import it.polimi.genomics.GMQLServer.Implementation
import it.polimi.genomics.core.DAG._
import it.polimi.genomics.core.DataStructures.ExecutionParameters.BinningParameter
import it.polimi.genomics.core.DataStructures._
import it.polimi.genomics.core.GDMSUserClass.GDMSUserClass
import it.polimi.genomics.core.ParsingType.PARSING_TYPE
import it.polimi.genomics.core._
import it.polimi.genomics.repository.federated.communication._
import it.polimi.genomics.repository.federated.{GF_Communication, GF_Interface}
import it.polimi.genomics.repository.{Utilities => General_Utilities}
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import it.polimi.genomics.spark.implementation.loaders.CustomParser
import org.apache.spark.launcher.{SparkAppHandle, SparkLauncher}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.Map
import scala.util.Random


trait FederatedStep

case class Move(from: Instance, to: Instance) extends FederatedStep

case class LocalExecute(iRVariable: IRVariable) extends FederatedStep

case class RemoteExecute(iRVariable: IRVariable, instance: Instance) extends FederatedStep

class FederatedImplementation(val launcherMode: String,
                              val tempDir: Option[String] = None,
                              val jobId: Option[String] = None,
                              val username: Option[String] = None,
                              val userClass: Option[GDMSUserClass] = None,
                              val sparkHome: Option[String] = None,
                              val cliJarLocal: Option[String] = None,
                              val masterClass: Option[String] = None,
                              val sparkCustomOption: Option[Map[String, Map[GDMSUserClass.Value, String]]] = None,
                              var distributionPolicy: Option[DistributionPolicy] = None
                             ) extends Implementation with Serializable {

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


  def runSparkSubmit(irVars: List[IRVariable]) = {


    println(irVars.head.metaDag)
    println(irVars.head.regionDag)

    def getOutDsName(path: String): String = path.split("_").last.replaceAll("/", "").trim

    val dsName: String =
      irVars.head.metaDag match {
        case ir: IRStoreMD => getOutDsName(ir.path)
        case ir: IRStoreFedMD => ir.name
        case _ =>
          irVars.head.regionDag match {
            case ir: IRStoreRD => getOutDsName(ir.path)
            case ir: IRStoreFedRD => ir.name
            case _ => throw new Exception("UNKNOWN IR TYPE")
          }
      }


    val repPrefix = if (General_Utilities().GMQL_REPO_TYPE == General_Utilities().LOCAL) "file://" else ""

    val serializedDag = DAGSerializer.serializeDAG(DAGWrapper(irVars))


    val SPARK_HOME = sparkHome.get
    val HADOOP_CONF_DIR = General_Utilities().HADOOP_CONF_DIR
    val YARN_CONF_DIR = General_Utilities().HADOOP_CONF_DIR
    val GMQL_HOME = General_Utilities().GMQLHOME

    val GMQLjar: String = cliJarLocal.get
    val MASTER_CLASS = masterClass.get
    val APPID = "GMQL_" + Random.nextInt() + "_" + jobId

    val env = Map(
      "HADOOP_CONF_DIR" -> HADOOP_CONF_DIR,
      "YARN_CONF_DIR" -> YARN_CONF_DIR
    )

    var d = new SparkLauncher(env.asJava)
      .setSparkHome(SPARK_HOME)
      .setAppResource(GMQLjar)
      .setMainClass(MASTER_CLASS)
      //      .setConf("spark.driver.extraClassPath", Utilities().lib_dir_local + "/libs/*")
      .addAppArgs(
      "-username", username.get,
      "-jobid", jobId.get,
      //      "-outputFormat", job.gMQLContext.outputFormat.toString,
      //      "-outputCoordinateSystem", job.gMQLContext.outputCoordinateSystem.toString,
      "-logDir", General_Utilities().getLogDir(username.get),
      "-userLogDir", General_Utilities().getUserLogDir(username.get),
      "-devLogDir", General_Utilities().getDevLogDir(username.get),
      "-tempdirfed", tempDir.get)
      .setConf("spark.app.id", APPID)


    val dagPath = repPrefix + General_Utilities().getRepository().saveDagQuery(username.get, serializedDag, jobId.get + dsName + ".dag")

    d = d.addAppArgs("-dagpath", dagPath)


    val sparkCustom: Map[String, Map[GDMSUserClass.Value, String]] = sparkCustomOption.getOrElse(Map.empty)

    // Set user-category-dependent Spark properties, if any
    if (sparkCustom.nonEmpty) {

      for (spark_property <- sparkCustom.keys) {

        val allClass = GDMSUserClass.ALL
        val property = sparkCustom(spark_property)

        // Note: A property set for a specific user class overrides the same property possibly defined for all classes
        if (property.isDefinedAt(userClass.get)) {
          d = d.setConf(spark_property, property(userClass.get))
        } else if (property.isDefinedAt(allClass)) {
          d = d.setConf(spark_property, property(allClass))
        }
      }
    }


    val b = d.setVerbose(true).startApplication()


    println("SPARK_SUBMIT")
    while (!isFinished(b))
      Thread.sleep(500L)

    b
  }

  def isFinished(b: SparkAppHandle) = b.getState match {
    case SparkAppHandle.State.FINISHED | SparkAppHandle.State.KILLED | SparkAppHandle.State.FAILED => true
    case _ => false
  }


  /** Starts the execution */
  override def go(): Unit = {
    logger.debug("Starting a FEDERATED execution")
    val start_query_fed = System.currentTimeMillis
    implementation()
    val stop_query_fed = System.currentTimeMillis
    logger.info("Total response time: " + ((stop_query_fed - start_query_fed) / 1000) + "s.")

  }

  override def collect(iRVariable: IRVariable): Any = throw new NotImplementedError()

  override def take(iRVariable: IRVariable, n: Int): Any = throw new NotImplementedError()

  /** stop GMQL implementation (kill a job) */
  override def stop(): Unit = println("stop")

  /** given the name of a parser, returns it. It must at least provide a parser for the "default" name */
  override def getParser(name: String, dataset: String): GMQLLoaderBase = {
    println("getParser")
    println("name" + name)
    println("dataset" + dataset)

    if (dataset.contains(".") && !dataset.contains(":") && !dataset.contains("/")) {
      val schema: GMQLSchema = api.getSchema(dataset)
      val parser = new CustomParser

      parser.setSchema(schema)
      parser
    }
    else
      gse.getParser(name, dataset)
  }

  def call(irVars: List[IRVariable]) = {
    logger.info("Executing local query")
    gse.to_be_materialized ++= irVars
    gse.go()
  }

  def moving(federatedJobId: String, dsName: String, from: String, to: Option[String], toName: String) = {
    if (to.isDefined) {
      implicit val backend = HttpURLConnectionBackend()

      val uri = uri"${to.get}federated/import/${federatedJobId}/${dsName}/${from}"
      println(uri)
      println
      println
      println

      val ns = new NameServer()
      val ins = new GMQLInstances(ns)
      val token = ins.getToken(toName)

      val request = sttp
        .header("Accept", "application/xml")
        .header("X-AUTH-TOKEN", "FEDERATED-TOKEN")
        .header(ins.AUTH_HEADER_NAME_FN, ns.NS_INSTANCENAME)
        .header(ins.AUTH_HEADER_NAME_FT, token)
        .get(uri)

      try {
        val response = request.send()

        println(response)
        println(response.body)
        println(response.statusText)
        Some(token)
      } catch {
        case e: Exception => throw new GmqlFederatedException(e.getMessage)
      }
    } else {
      GF_Interface.instance().importDataset(federatedJobId, dsName, from)
      None
    }


  }

  def poolingMoving(federatedJobId: String, dsName: String, to: Option[String], token: Option[String]) = {
    if (to.isDefined) {


      implicit val backend = HttpURLConnectionBackend()

      var status = "started"
      val uri = uri"${to.get}federated/checkimport/${federatedJobId}/${dsName}"


      do {
        Thread.sleep(1000)

        val ns = new NameServer()
        val ins = new GMQLInstances(ns)

        val request = sttp
          .header("Accept", "application/xml")
          .header("X-AUTH-TOKEN", "FEDERATED-TOKEN")
          .header(ins.AUTH_HEADER_NAME_FN, ns.NS_INSTANCENAME)
          .header(ins.AUTH_HEADER_NAME_FT, token.get)
          .get(uri)

        try {
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
        } catch {
          case e: Exception => throw new GmqlFederatedException(e.getMessage)
        }

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


  def pooling(remoteServerUri: String, remoteJobId: String, token: String) = {
    implicit val backend = HttpURLConnectionBackend()

    var status = "started"

    do {
      Thread.sleep(1000)

      val uri = uri"${remoteServerUri}jobs/$remoteJobId/trace"

      val ns = new NameServer()
      val ins = new GMQLInstances(ns)

      val request = sttp
        .header("Accept", "application/xml")
        .header("X-AUTH-TOKEN", "FEDERATED-TOKEN")
        .header(ins.AUTH_HEADER_NAME_FN, ns.NS_INSTANCENAME)
        .header(ins.AUTH_HEADER_NAME_FT, token)
        .get(uri)

      try {
        val response = request.send()

        if (response.code != 200) {
          throw new GmqlFederatedException("Fatal federated error: trace response code != 200 ")
        }

        status = (scala.xml.XML.loadString(response.body.right.get) \\ "status").text

        if (status.contains("FAILED") || status.contains("STOPPED"))
          throw new GmqlFederatedException("Fatal federated error: trace-> " + status)

        println("\t\t\t\t\t" + remoteJobId + status)
      } catch {
        case e: Exception => throw new GmqlFederatedException(e.getMessage)
      }

    } while (!status.equals("EXEC_SUCCESS"))
  }

  def callRemote(irVars: List[IRVariable], instance: GMQLInstance) = {
    implicit val backend = HttpURLConnectionBackend()

    val remoteServerUri = GF_Communication.instance().getLocation(instance.name).URI


    val serilizedDag = DAGSerializer.serializeDAG(DAGWrapper(irVars))


    val uri = uri"${remoteServerUri}queries/dag/tab?federatedJobId=$jobId"

    val ns = new NameServer()
    val ins = new GMQLInstances(ns)
    val token = ins.getToken(instance.name)

    val request = sttp.body(serilizedDag)
      .header("Accept", "application/xml")
      .header("X-AUTH-TOKEN", "FEDERATED-TOKEN")
      .header(ins.AUTH_HEADER_NAME_FN, ns.NS_INSTANCENAME)
      .header(ins.AUTH_HEADER_NAME_FT, token)
      .post(uri)

    logger.info(s"Sending sub-query to $instance")
    try {
      val response = request.send()

      if (response.code != 200) {
        throw new GmqlFederatedException("Fatal federated error response code  != 200 ")
      }

      val remoteJobId = (scala.xml.XML.loadString(response.body.right.get) \\ "id").text

      //add waiting execution
      pooling(remoteServerUri, remoteJobId, token)


      //add move
      val remoteDsName =
        irVars.head.regionDag match {
          case federated: Federated => federated.name
          case _ => irVars.head.metaDag.asInstanceOf[Federated].name
        }


      val fedIrVars: List[IROperator] = irVars.flatMap(irVar =>
        irVar.metaDag.getDependencies ++ irVar.regionDag.getDependencies
      ).filter(_.isInstanceOf[Federated])

      logger.debug("\t\t\t\t\t\t\t" + fedIrVars)
      println(response.body)

    } catch {
      case e: Exception => throw new GmqlFederatedException(e.getMessage)
    }
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


    val irVarFiltered = irVars.filter { x =>
      !(x.regionDag.isInstanceOf[IRNoopRD] && x.metaDag.isInstanceOf[IRStoreMD])
    }


    if (!previouslyRunDag.contains(executionDag)) {
      //send other
      whereExDag match {
        case LOCAL_INSTANCE =>
          println("call(irVarFiltered)")
          val start = System.currentTimeMillis
          if (launcherMode == "LOCAL") {
            call(irVarFiltered)
          }
          else {
            runSparkSubmit(irVarFiltered)
          }
          val time = (System.currentTimeMillis - start) / 1000
          logger.info("Execution time at local: " + time + "s.")
        case _: GMQLInstance =>

          println("callRemote(irVarFiltered)")
          //TODO
          //        call(irVarFiltered)

          val start = System.currentTimeMillis
          callRemote(irVarFiltered, whereExDag)
          val time = (System.currentTimeMillis - start) / 1000
          logger.info("Execution time at remote(" + whereExDag +  "): " + time + "s.")

      }
      previouslyRunDag.add(executionDag)
    }

    val dssNamesToCopy: Seq[String] = executionDag.dag.flatMap(_.roots).filter(_.isInstanceOf[Federated]).map(_.asInstanceOf[Federated].name)

    val remoteServerUriOpt =
      if (destination != LOCAL_INSTANCE)
        Some(GF_Communication.instance().getLocation(destination.name).URI)
      else
        None

    val startMoving = System.currentTimeMillis
    dssNamesToCopy.foreach { dsName =>
      logger.info("Moving " + dsName + " from " + whereExDag + " to " + destination)

      val from = whereExDag match {
        case LOCAL_INSTANCE => new NameServer().NS_INSTANCENAME
        case _ => whereExDag.name
      }
      val tokenOpt = moving(jobId.get, dsName, from, remoteServerUriOpt, destination.name)
      poolingMoving(jobId.get, dsName, remoteServerUriOpt, tokenOpt)
    }
    val timeMoving = (System.currentTimeMillis - startMoving) / 1000
    if(dssNamesToCopy.nonEmpty)
      logger.info("Execution time of moving(" +  " from " + whereExDag + " to " + destination +  "): " + timeMoving + "s.")

  }

  def implementation(): Unit = {
    val opDAG = new OperatorDAG(to_be_materialized.flatMap(x => List(x.metaDag, x.regionDag)).toList)
    if(this.distributionPolicy.isDefined) {
      logger.info("Applying Distribution Policy")
      distributionPolicy.get.assignLocations(opDAG)
    }
    logger.info(s"Starting Federated query $jobId")
    //val opDAGFrame = new OperatorDAGFrame(opDAG)
    //    showFrame(opDAGFrame, "OperatorDag")

    //TODO check .get
    logger.info("Splitting the computation DAG")
    val dagSplits = DAGManipulator.splitDAG(opDAG, jobId.get, tempDir.get)
    logger.info("Getting DAGs to execute remotely")
    val executionDAGs = DAGManipulator.generateExecutionDAGs(dagSplits.values.toList)

    //val f2 = new MetaDAGFrame(executionDAGs)
    //    showFrame(f2, "ExDag")

    logger.info("Starting the federated query")
    executionDAGs.roots.foreach(recursiveCall(_, LOCAL_INSTANCE))
  }

  override def collectIterator(iRVariable: IRVariable): (Iterator[(GRecordKey, Array[GValue])], Iterator[(Long, (String, String))], List[(String, PARSING_TYPE)]) =
    throw new NotImplementedError()

  override def takeFirst(iRVariable: IRVariable, n: Int): (Array[(GRecordKey, Array[GValue])], Array[(Long, (String, String))], List[(String, PARSING_TYPE)]) =
    throw new NotImplementedError()
}


