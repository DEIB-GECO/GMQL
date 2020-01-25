package it.polimi.genomics.manager

import java.io.File
import java.nio.file.Paths
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.TimeUnit

import it.polimi.genomics.GMQLServer.GmqlServer
import it.polimi.genomics.compiler._
import it.polimi.genomics.core.DAG._
import it.polimi.genomics.core.DataStructures.ExecutionParameters.BinningParameter
import it.polimi.genomics.core.DataStructures._
import it.polimi.genomics.core.{GMQLSchemaCoordinateSystem, GMQLSchemaFormat, GMQLScript}
import it.polimi.genomics.core.Debug.{DAGInjector, EPDAG, EPDAGDraw, EPDAGFrame}
import it.polimi.genomics.manager.Launchers.{GMQLLauncher, GMQLLocalLauncher}
import it.polimi.genomics.manager.Status._
import it.polimi.genomics.repository.FSRepository.FS_Utilities.{deleteDFSDir, deleteFromLocalFSRecursive}
import it.polimi.genomics.repository.FSRepository.{FS_Utilities => FSR_Utilities}
import it.polimi.genomics.repository.GMQLExceptions.GMQLNotValidDatasetNameException
import it.polimi.genomics.repository.{GMQLRepository, RepositoryType, Utilities => General_Utilities}
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.immutable.HashMap
import scala.collection.mutable


/**
  * Created by Abdulrahman Kaitoua on 10/09/15.
  * Email: abdulrahman.kaitoua@polimi.it
  *
  */
/**
  * GMQL Job
  * Hold the state of the job, the configuration of which this job runs on.
  *
  * @param gMQLContext [[GMQLContext]]  sets the implementation type and the defaults for binning
  * @param script      [[GMQLScript]] contains the script string and the script path
  * @param username    [[String]] as the executing user of this job
  */
class GMQLJob(val gMQLContext: GMQLContext, val script: GMQLScript, val username: String, val federated: Boolean = false) {

  private final val date: String = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());


  def queryName: String = {
    val fileName = new java.io.File(script.scriptPath).getName
    val name = if (fileName.indexOf(".") > 0) fileName.substring(0, fileName.indexOf(".")) else fileName
    name
  }

  def jobId: String = {
    val res = generateJobId(username, queryName)
    println("Called, returning jobiid "+res)
    res
  }



  private final val logger: Logger = LoggerFactory.getLogger(this.getClass);
  final val loggerPath = General_Utilities().getLogDir(username) + jobId.toLowerCase() + ".log"
  final val userLoggerPath = General_Utilities().getUserLogDir(username) + jobId.toLowerCase() + ".log"
  final val devLoggerPath = General_Utilities().getDevLogDir(username) + jobId.toLowerCase() + ".log"
  val jobOutputMessages: StringBuilder = new StringBuilder

  //  var script = fixFileFormat(scriptPath)
  //  val regionsURL = if (General_Utilities().MODE == FS_Utilities.HDFS) FS_Utilities.getInstance().HDFSRepoDir else FS_Utilities.getInstance().RepoDir
  /**
    * Elapsed time in milliseconds
    *
    * @param compileTime   compile time of the query in milliseconds
    * @param executionTime execution time of query in milliseconds
    * @param createDsTime  dataset creation time at the end of execution in milliseconds
    */
  case class ElapsedTime(var compileTime: Long = -1L, var executionTime: Long = -1L, var createDsTime: Long = -1L)

  val elapsedTime: ElapsedTime = ElapsedTime()

  var status: Status.Value = PENDING
  var outputVariablesList: List[String] = List[String]()
  val server: GmqlServer = new GmqlServer(gMQLContext.implementation, Some(gMQLContext.binSize.Map))
  val translator: Translator = new Translator(server, "")
  var operators: List[Operator] = List[Operator]()
  var inputDataSets: Map[String, String] = new HashMap;
  var submitHandle: GMQLLauncher = null
  var repositoryHandle: GMQLRepository = gMQLContext.gMQLRepository
  var DAG: mutable.MutableList[IRVariable] = mutable.MutableList[IRVariable]()

  /**
    * Compile GMQL Job,
    * Extract the output dataset names,
    * Find the input datasets in the repository,
    * Modify DAG to include paths instead of Dataset names for both input and output.
    *
    * @param id [[ String]] as the Job ID
    * @return a Tuple of a [[ String]] as the Job ID, and a List of Strings as the output Datasets.
    */
  def compile(id: String = jobId): (String, List[String]) = {
    status = Status.COMPILING
    General_Utilities().USERNAME = username

    val res_name = generateResultName(queryName)

    val compileTimestamp = System.currentTimeMillis();
    try {
      //compile the GMQL Code phase 1
      val languageParserOperators = translator.phase1(script.script)

      //Get the output Datasets names.
      outputVariablesList = languageParserOperators.flatMap(x => x match {
        case d: MaterializeOperator =>
          if (!d.store_path.isEmpty)
            Some(res_name + "_" + d.store_path.replace("/", "_"))
          else Some(res_name)

        case _ => None
      })

      // Get the input Dataset names from the Select Statements
      inputDataSets = languageParserOperators.flatMap { x =>
        x match {
          case select: SelectOperator =>

            val loc = select.operator_location

            //            logger.info(select.op_pos + "\t" + select.output + "\t" + select.parameters);
            val DSname: String = select.input1 match {
              case p: VariablePath => p.path
              case p: Variable => p.name
            }
            if (repositoryHandle.DSExists(DSname, username, loc)) {
              val user = if (loc.getOrElse(LOCAL_INSTANCE) != LOCAL_INSTANCE) "federated" else if (repositoryHandle.DSExistsInPublic(DSname)) "public" else this.username
              Some(DSname, getRegionFolder(DSname, user, loc))
            } else {
              logger.warn(DSname + " is not a dataset in the repository...error");
              None
            }
          case s: Operator => None
        }
      }.toMap

      val fsRegDir =
        if (General_Utilities().GMQL_REPO_TYPE == General_Utilities().LOCAL) {
          General_Utilities().getRegionDir(username)
        } else {
          General_Utilities().getHDFSNameSpace() + General_Utilities().getHDFSRegionDir()
        }

      //      val fsRegDir: String =
      //        General_Utilities().getHDFSNameSpace() + General_Utilities().getHDFSRegionDir()


      //extract the select and materialize operators, change the Store path to be $JobID_$StorePath
      operators = languageParserOperators.map(x => x match {
        case select: SelectOperator => logger.info(select.op_pos + "\t" + select.output + "\t" + select.parameters);
          val dsinput = select.input1 match {
            case p: VariablePath =>
              if (repositoryHandle.DSExists(p.path, username)) {
                val user = if (repositoryHandle.DSExistsInPublic(p.path)) "public" else this.username
                //todo: find a better way to avoid accesing hdfs at compilation time
                val newPath =
                  if (Utilities().LAUNCHER_MODE equals Utilities().REMOTE_CLUSTER_LAUNCHER)
                    General_Utilities().getSchemaDir(user) + p.path + ".xml"
                  else getRegionFolder(p.path, user)
                println(newPath)
                new VariablePath(newPath, p.parser_name);
              } else {
                p
              }
            case p: VariableIdentifier => {
              if (repositoryHandle.DSExists(p.IDName, username)) {
                val user = if (repositoryHandle.DSExistsInPublic(p.IDName)) "public" else this.username
                val newPath =
                  if (Utilities().LAUNCHER_MODE equals Utilities().REMOTE_CLUSTER_LAUNCHER)
                    General_Utilities().getSchemaDir(user) + p.IDName + ".xml"
                  else getRegionFolder(p.IDName, user)
                new VariableIdentifier(newPath);
              } else {
                p
              }
            }
          }
          new SelectOperator(select.op_pos, dsinput, select.input2, select.output, select.parameters)
        /*val ds = new IRDataSet(DSname, List[(String, PARSING_TYPE)]().asJava)
        if (repositoryHandle.DSExists(ds, username)) {
          val user = if (repositoryHandle.DSExistsInPublic(ds)) "public" else this.username
          Some(DSname, getHDFSRegionFolder(DSname, user))
        } else {
          logger.warn(DSname + " is not a dataset in the repository...error");
          None
        }*/
        case d: MaterializeOperator =>
          logger.info("d: MaterializeOperator id: " + id)
          if (!d.store_path.isEmpty) {
            logger.info("d: MaterializeOperator d.store_path: " + d.store_path)
            if (General_Utilities().MODE == General_Utilities().HDFS)
              d.store_path = fsRegDir + "/" + res_name + "_" + d.store_path + "/"
            else d.store_path = General_Utilities().getRegionDir(username) + "/" + res_name + "_" + d.store_path + "/"
            //else d.store_path = res_name + "_" + d.store_path + "/"
            d
          }
          else {
            if (General_Utilities().MODE == General_Utilities().HDFS)
              d.store_path = fsRegDir + "/" + res_name + "/"
            else //(General_Utilities().MODE == General_Utilities().LOCAL)
              d.store_path = General_Utilities().getRegionDir(username) + "/" + res_name + "/"
            //else d.store_path = res_name + "/"
            d
          }
        case s: Operator => s
      })

      //Perform the second compilation phase
      if (translator.phase2(operators))
        status = Status.COMPILE_SUCCESS
      else
        status = Status.COMPILE_FAILED

      // Inject debug operators in the DAG
      if(Utilities().DEBUG_MODE)
        server.materializationList = DAGInjector.inject(server.materializationList, Utilities().DRAW_DAG)

      DAG = server.materializationList

      if(Utilities().DEBUG_MODE && Utilities().DRAW_DAG) {
        val vd = new VariableDAG(DAG.toList)
        val operatorDAGFrame = new OperatorDAGFrame(vd.toOperatorDAG, squeeze = true)
        DAGDraw.showFrame(operatorDAGFrame, "Operator DAG")
      }


      if(Utilities().DEBUG_MODE && Utilities().DRAW_DAG) {
        val epdag = EPDAG.build(DAG.toList)
        val frame = new EPDAGFrame(epdag)
        EPDAGDraw.showFrame(frame, "Execution Profile DAG")
      }

    } catch {
      case e: CompilerException => status = Status.COMPILE_FAILED; logError(e.getMessage); e.printStackTrace()
      case ex: Exception => status = Status.COMPILE_FAILED; logError(ex.getMessage); ex.printStackTrace()
    }

    elapsedTime.compileTime = System.currentTimeMillis() - compileTimestamp

    (id, outputVariablesList)
  }


  /**
    * It renames the paths specified in the serialized DAG to HDFS paths and
    * builds a new serialized DAG with the modifications
    *
    * @param dagString : string of the serialized DAG
    * @return A list of the output directories and the serialized DAG with the paths renamed
    **/
  def renameDAGPaths(dagString: String, federatedJobId: Option[String] = None): (List[String], String) = {
    val dagVars: List[IRVariable] = DAGSerializer.deserializeDAG(dagString).dag
    val outDss = dagVars.flatMap(dagVar => rec(dagVar.metaDag, federatedJobId) ++ rec(dagVar.regionDag, federatedJobId)).distinct
    //Get the output Datasets names.
    outputVariablesList = outDss

    (outDss, DAGSerializer.serializeDAG(DAGWrapper(dagVars)))
  }


  /**
    * Given a dataset name, it returns its name for the current job.
    * Used for specifying the output dataset name
    *
    * @param x : output dataset name
    * @return the real output datset name given the current job
    **/
  def renameOutputDirs(x: String): String = {
    val repPrefix = if(General_Utilities().GMQL_REPO_TYPE == General_Utilities().LOCAL) "file://" else ""
    val fsRegDir = FSR_Utilities.gethdfsConfiguration().get("fs.defaultFS") +
      General_Utilities().getHDFSRegionDir(this.username)
    val dir = if (General_Utilities().MODE == General_Utilities().HDFS)
      fsRegDir + x + "/"
    else repPrefix + General_Utilities().getRegionDir(this.username) + x + "/"
    dir
  }


  /**
    * Recursive search of READ and STORE operations in the DAG.
    * It searches recursively in all the IROperators of the DAG, the ones related
    * with READ and STORE operations and changes the names of the dataset to their
    * actual path.
    *
    * @param inp : IROperator representing the dag
    * @return a list of output dataset names
    **/
  def rec(inp: IROperator, federatedJobId: Option[String] = None): List[String] = {
    val result = inp match {
      case x: IRReadRD[_, _, _, _] =>
        x.paths = List(getInputDsPath(x.dataset.position))
        None
      case x: IRReadMD[_, _, _, _] =>
        x.paths = List(getInputDsPath(x.dataset.position))
        None
      case x: IRStoreRD =>
        val outDsName = generateResultName(queryName) + "_" + x.dataSet.position
        x.path = renameOutputDirs(outDsName)
        Some(outDsName)
      case x: IRStoreMD =>
        val outDsName = generateResultName(queryName) + "_" + x.dataSet.position
        x.path = renameOutputDirs(outDsName)
        Some(outDsName)

      //FEDERATED
      case x: Federated =>
        if (federatedJobId.isDefined) {
          val path = federatedJobId.get + "/" + x.name
          x.path = Some(renameOutputDirs(path))
        }
        None
      case _ =>
        None
    }
    val tempRes = inp.getDependencies.flatMap(operator => rec(operator, federatedJobId))

    tempRes ++ List(result).flatten
  }

  /**
    * Given a dataset name, it returns its path.
    *
    * @param inDs : dataset name, possibly with namespace
    * @return path of x
    **/
  def getInputDsPath(inDs: String) = {
    //TODO correct
    //    if (inputDs.startsWith("/"))
    //      inputDs
    //    else {
    val inputDs = inDs.split('.').last
    val user = if (repositoryHandle.DSExistsInPublic(inputDs)) "public" else this.username
    val repPrefix = if(General_Utilities().GMQL_REPO_TYPE == General_Utilities().LOCAL) "file://" else ""

    val newPath =
      if (Utilities().LAUNCHER_MODE equals Utilities().REMOTE_CLUSTER_LAUNCHER)
        General_Utilities().getSchemaDir(user) + inputDs + ".xml"
      else
        repPrefix + getRegionFolder(inputDs, user) + "/"
    newPath + "/"
    //    }
  }


  /**
    * return the input dataset directory.
    *
    * @param dsName string of the data name
    * @param user   String of the user name, owner of the dataset
    * @return String of the dataset location
    */
  def getRegionFolder(dsName: String, user: String, location: Option[GMQLInstance] = None): String = {
    location match {
      case Some(LOCAL_INSTANCE) | None =>
        val path = repositoryHandle.listDSSamples(dsName, user).asScala.head.name //(xml \\ "url") (0).text

        val (location, ds_origin) = repositoryHandle.getDSLocation(dsName, user)
        if (location == RepositoryType.HDFS)
          getHDFSRegionFolder(path, user)
        else //DatasetOrigin.IMPORTED
          Paths.get(path).getParent.toString
      case Some(Instance(_)) =>
        dsName
    }


  }

  /**
    *
    * return the HDFS directory for a specific folder, including the file system name
    *
    * @param path [[ String]] of the dataset
    * @param user [[ String]] as the user name, owner of the dataset
    * @return String of the absolute path of the dataset folder in HDFS
    */
  def getHDFSRegionFolder(path: String, user: String): String = {
    if (path.startsWith("hdfs"))
      (new org.apache.hadoop.fs.Path(path)).getParent.toString
    else
      General_Utilities().getHDFSNameSpace() +
        General_Utilities().getHDFSRegionDir(user) + Paths.get(path).getParent.toString
  }


  /**
    *
    * Run GMQL Job using the laucher specified in the input parameters.
    *
    * @param id         GMQL Job ID
    * @param submitHand [[ GMQLLauncher]] as the launcher to use to run GMQL Job
    * @return [[ State]] of the running job
    */
  def runGMQL(id: String = jobId, submitHand: GMQLLauncher = new GMQLLocalLauncher(this)): Status.Value = {
    this.status = Status.PENDING

    this submitHandle = submitHand


    this.submitHandle run

    //timer to find the execution time
    val timer = new Thread(new Runnable {
      def run() {
        var timer = 0
        status = getExecJobStatus
        while (status.equals(Status.PENDING)) {
          timer = timer + 1
          Thread.sleep(500l)
          status = getExecJobStatus
          if (timer == 100)
            status = EXEC_FAILED;
          logger.info(jobId + "\t" + status)
        }
        val timestamp = System.currentTimeMillis();
        while (status.equals(Status.RUNNING)) {
          Thread.sleep(500l)
          status = getExecJobStatus
          println(jobId + "\t" + status)
        }
        elapsedTime.executionTime = System.currentTimeMillis() - timestamp

        logger.info(jobId + "\t" + getJobStatus)

        if (status == Status.EXEC_SUCCESS) {
          logger.info("Creating dataset..." + outputVariablesList)
          if (!federated)
            createds()
          logger.info("Creating dataset Done...")
        }

        if (status == Status.EXEC_STOPPED || status == Status.EXEC_FAILED) {
          logger.info("Execution stopped. Any residual dataset will be deleted.")
          deleteResidualDs()
        }

      }
    }).start()

    logger.info("Execution Time: " + (elapsedTime.executionTime / 1000))

    status
  }

  /**
    * Delete any residual dataset created by a Failed or Stopped  execution
    *
    * @return
    */
  def deleteResidualDs(): Unit = {

    if (outputVariablesList.nonEmpty) {

      outputVariablesList.foreach(ds => {

        logger.info("Removing residual dataset: " + ds)

        try {

          if (General_Utilities().GMQL_REPO_TYPE == General_Utilities().HDFS) {

            val path = General_Utilities().getHDFSRegionDir(username) + "/" + ds
            deleteDFSDir(path)

          } else if (General_Utilities().GMQL_REPO_TYPE == General_Utilities().LOCAL) {

            val file = new File(General_Utilities().getRegionDir() + "/" + General_Utilities().getRegionDir(username))
            if (file.exists())
              deleteFromLocalFSRecursive(file)

          } else {
            logger.warn("No region folder deleted, repo type is unknown.")
          }

        } catch {
          case ex: Exception => ex.printStackTrace()
        }

      })
    } else {
      logger.info("No residual dataset found.")
    }

  }

  /**
    * Create a data set in the repository with the result of the GMQL job
    */
  def createds(): Unit = {
    val dsCreationTimestamp = System.currentTimeMillis();
    this.status = Status.DS_CREATION_RUNNING
    if (!outputVariablesList.isEmpty) {
      try {


        // Compute some execution-related dataset metadata
        val hours = TimeUnit.MILLISECONDS.toHours(elapsedTime.executionTime)
        val minutes = TimeUnit.MILLISECONDS.toMinutes(elapsedTime.executionTime) - TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(elapsedTime.executionTime))
        val seconds = TimeUnit.MILLISECONDS.toSeconds(elapsedTime.executionTime) - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(elapsedTime.executionTime))
        val execTime = "%02d:%02d:%02d".format(hours, minutes, seconds)

        val dsmeta = Map("Query name" -> this.queryName, "Execution time" -> execTime)

        println("outputVariablesList quiii")
        outputVariablesList.foreach(println)

        outputVariablesList.map { ds =>

          val (samples, sch) = repositoryHandle.listResultDSSamples(ds + "/files/", this.username)

          //          println("samples")
          //          samples.asScala foreach println _


          repositoryHandle.createDs(new IRDataSet(ds, sch.asScala.map(x => (x.name, x.fieldType)).toList.asJava),
            this.username, samples, script.scriptPath,
            if (gMQLContext.outputFormat.equals(GMQLSchemaFormat.GTF)) GMQLSchemaFormat.GTF else GMQLSchemaFormat.TAB,
            if (gMQLContext.outputCoordinateSystem.equals(GMQLSchemaCoordinateSystem.ZeroBased)) GMQLSchemaCoordinateSystem.ZeroBased
            else if (gMQLContext.outputCoordinateSystem.equals(GMQLSchemaCoordinateSystem.OneBased)) GMQLSchemaCoordinateSystem.OneBased
            else GMQLSchemaCoordinateSystem.Default, dsmeta)
        }
        elapsedTime.createDsTime = System.currentTimeMillis() - dsCreationTimestamp
        logger.info("DataSet creation Time: " + (elapsedTime.createDsTime / 1000))

        this.status = Status.SUCCESS


        //@andrea
        if( Utilities().DEBUG_MODE && server.implementation.isInstanceOf[GMQLSparkExecutor] ){
          val ePDAG = server.implementation.asInstanceOf[GMQLSparkExecutor].ePDAG
          ePDAG.executionEnded()

          ePDAG.computeCriticalPath()


          if(Utilities().DRAW_DAG) {
            val frame = new EPDAGFrame(ePDAG)
            EPDAGDraw.showFrame(frame, "Final EPDAG")
          }


          ePDAG.save(jobId, General_Utilities().getDDagDir(username))


        } else {
          println("not an instance of GMQLSparkExecutor, instead: "+server.implementation.getClass.getName)
        }




      } catch {
        case ex: GMQLNotValidDatasetNameException => status = Status.DS_CREATION_FAILED; logger.error("The Dataset name is not valid..."); ex.printStackTrace(); throw ex
        case ex: Exception => this.status = Status.DS_CREATION_FAILED; logError("Input Dataset is empty , (error in process or Wrong input Selection query)."); ex.printStackTrace(); throw ex
        case ex: Throwable => this.status = Status.DS_CREATION_FAILED; logError("Throwable error "); ex.printStackTrace(); throw ex
      }
    }
    else
      throw new RuntimeException("The code is not compiled..")
  }

  /**
    *
    * add logging information to the logger and to the returned message to the user.
    *
    * @param message logged message as a String
    */
  private def logInfo(message: String): Unit = {
    logger.info(message)
    jobOutputMessages.append(message)
  }

  /**
    * Log the error messages to the logger and to the returned message to the user.
    *
    * @param message String of the message to be logged
    */
  private def logError(message: String): Unit = {
    logger.error(message)
    jobOutputMessages.append(message)
  }

  /**
    * @return the execution time in milliseconds
    */
  def getExecutionTime(): Long = elapsedTime.executionTime

  /**
    * @return the Compilation time in milliseconds
    */
  def getCompileTime(): Long = elapsedTime.compileTime

  /**
    * @return the DataSet creation time in milliseconds
    */
  def getDSCreationTime(): Long = elapsedTime.createDsTime

  /**
    * @return the user message about the exceution (copy of the log).
    */
  def getMessage() = jobOutputMessages.toString()

  /**
    * @return the status [[ Status]] of the GMQL Job
    */
  def getJobStatus: Status.Value = this.synchronized {
    this.status
  }

  /**
    * @return the status [[ Status]] of the GMQL Job.
    *         This status is the executor status.
    */
  def getExecJobStatus: Status.Value = this.synchronized {
    if (submitHandle != null) {
      this.submitHandle.getStatus()
    }
    else Status.PENDING
  }

  /**
    * return all the log strings of a specific job.
    *
    * @return list of Strings of the log information.
    */
  def getLog: List[String] = {
    submitHandle.getLog()
  }

  /**
    * Generate GMQL Job ID by concatinating the username with the script file name,
    * and the time stamp.
    *
    * @param username  String of the username (the owner of the Job)
    * @param queryname String of the Script File name
    * @return GMQL Job ID as a string
    */
  def generateJobId(username: String, queryname: String): String = {
    "job_" + username + "_" + generateResultName(queryname)
  }


  def generateResultName(queryname: String = queryName): String = {
    queryname.toLowerCase() + "_" + date
  }

}
