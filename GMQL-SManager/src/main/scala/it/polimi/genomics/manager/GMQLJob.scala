package it.polimi.genomics.manager

import java.io._
import java.nio.file.Paths

import it.polimi.genomics.compiler._
import it.polimi.genomics.GMQLServer.{Implementation, GmqlServer}
import it.polimi.genomics.core.DataStructures.{IRDataSet, IRVariable}
import it.polimi.genomics.core.ParsingType
import it.polimi.genomics.core.ParsingType._
import it.polimi.genomics.manager.Launchers.{GMQLLocalLauncher, GMQLLauncher}
import it.polimi.genomics.repository.FSRepository.DFSRepository
import it.polimi.genomics.repository.GMQLRepository.{GMQLSchemaTypes, GMQLRepository}
import it.polimi.genomics.repository.RepositoryManagerV2
import it.polimi.genomics.repository.datasets.GMQLNotValidDatasetNameException
import it.polimi.genomics.repository.util.Utilities
import org.apache.hadoop.fs.{Path, FileSystem}
import org.slf4j.LoggerFactory
import java.util.Date;
import java.text.SimpleDateFormat;

import scala.collection.JavaConverters._
import Status._

import scala.collection.immutable.HashMap
import scala.collection.mutable
import scala.xml.XML

/**
 * Created by Abdulrahman Kaitoua on 10/09/15.
 * Email: abdulrahman.kaitoua@polimi.it
 *
 */
class GMQLJob(val implementation: Implementation,val binSize: Long,val scriptPath: String, val script:String, val username: String = Utilities.USERNAME, val outputFormat:String) {


  val date = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
  def jobId: String = generateJobId(username, new java.io.File(scriptPath).getName.substring(0, new java.io.File(scriptPath).getName.indexOf(".")))
  private final val logger = LoggerFactory.getLogger(this.getClass);
  val jobOutputMessages = new StringBuilder
  val Username = {
    username
  }

//  var script = fixFileFormat(scriptPath)
  val regionsURL = if (Utilities.getInstance().MODE == Utilities.HDFS) Utilities.getInstance().HDFSRepoDir else Utilities.getInstance().RepoDir
  case class ElapsedTime (var compileTime:String,var executionTime:String,var createDsTime:String =" Included in The execution time. (details in the log File)")
  val elapsedTime =ElapsedTime("","","")

  var status = PENDING
  var outputVariablesList = List[String]()
  val server = new GmqlServer(implementation, Some(binSize))
  val translator = new Translator(server, "")
  var operators: List[Operator] = List[Operator]()
  var inputDataSets:Map[String, String] = new HashMap;
  var submitHandle: GMQLLauncher = null
  var repositoryHandle: GMQLRepository = null
  var DAG: mutable.MutableList[IRVariable] = mutable.MutableList[IRVariable]()

  def compile(id:String = jobId): (String, List[String]) = {
    status = Status.COMPILING
    Utilities.USERNAME = Username

    val Compiletimestamp = System.currentTimeMillis();
    try {
      //compile the GMQL Code
      val languageParserOperators = translator.phase1(script)

      //Create the datasets names based on the materialized dataset names
      outputVariablesList = languageParserOperators.flatMap(x => x match {
        case d: MaterializeOperator =>
          if (!d.store_path.isEmpty)
            Some(id + "_" + d.store_path.replace("/", "_"))
          else Some(id)
        case _ => None
      })

      inputDataSets = languageParserOperators.flatMap { x => x match {
        case select: SelectOperator => select.input1 match {
          case p: Variable =>
            if (Utilities.getInstance().checkDSNameinRepo(this.username, p.name)) {
              val user = if (Utilities.getInstance().checkDSNameinPublic(p.name)) "public" else this.username
              Some(p.name, getDSFolder(p.name,user))
            } else {
              logger.warn(p.name+" is not a dataset in the repository...")
              None
            }
          case p:Variable => None
        }
        case s: Operator => None
      }}.toMap


      operators = languageParserOperators.map(x => x match {
        case d: MaterializeOperator =>
          if (!d.store_path.isEmpty){
            d.store_path = id + "_" + d.store_path + "/"
            d
          }
            //MaterializeOperator(d.op_pos, id + "_" + d.store_path + "/", d.input1, d.input2);
          else{
            d.store_path = id + "/"
            d
          }
            //MaterializeOperator(d.op_pos, id + "/", d.input1, d.input2);
        case s: Operator => s
      })

      if(translator.phase2(operators))
        status = Status.COMPILE_SUCCESS
      else
        status = Status.COMPILE_FAILED
      DAG = server.materializationList
    } catch {
      case e: CompilerException => status = Status.COMPILE_FAILED; logError(e.getMessage)
      case ex: Exception => status = Status.COMPILE_FAILED; logError(ex.getMessage)
    }

    elapsedTime.compileTime = ((System.currentTimeMillis() - Compiletimestamp)/1000).toString;

    (id, outputVariablesList)
  }

  def getDSFolder(dsName:String,user:String): String ={
    val xmlFile = Utilities.getInstance().RepoDir + user + "/datasets/" + dsName + ".xml"
   val xml = XML.loadFile(xmlFile)
    val path = (xml \\ "url")(0).text
    if(path.startsWith("hdfs")) (new org.apache.hadoop.fs.Path(path)).getParent.toString else
    Utilities.getInstance.HDFSRepoDir+"/"+user+"/regions/"+Paths.get(path).getParent.toString
  }

  def runGMQL(id:String = jobId,submitHand: GMQLLauncher= new GMQLLocalLauncher(this), repoHandler:GMQLRepository = new DFSRepository()):Status.Value = {
    this.status = Status.RUNNING

    this repositoryHandle = repoHandler
    this submitHandle= submitHand
    this.submitHandle run

    //timer to find the execution time
    val timer = new Thread(new Runnable {
      def run() {
        var timer = 0
        status = getExecJobStatus
        while (status.equals(Status.PENDING)){
          timer = timer + 1
          Thread.sleep(500l)
          status = getExecJobStatus
          if(timer == 100)
            status = EXEC_FAILED;
          logger.info(jobId+"\t"+status)
        }
        val timestamp = System.currentTimeMillis();
        while (status.equals(Status.RUNNING)){
          Thread.sleep(500l)
          status = getExecJobStatus
          logger.info(jobId+"\t"+status)
        }
        elapsedTime.executionTime = ((System.currentTimeMillis() - timestamp) / 1000).toString

//        status = Status.DS_CREATION_RUNNING
//
        logger.info(jobId+"\t"+getJobStatus)
        if(status == Status.EXEC_SUCCESS) {
          logger.info("Creating dataset..." + outputVariablesList)
          createds()
          logger.info("Creating dataset Done...")
        }
//        status = getJobStatus//Status.SUCCESS

      }
    }).start()

    logger.info("Execution Time: "+elapsedTime.executionTime)

    status
  }

  def createds(): Unit = {
    val dstimestamp = System.currentTimeMillis();
    this.status = Status.DS_CREATION_RUNNING
    if (!outputVariablesList.isEmpty) {
      try {
        outputVariablesList.map { ds =>

          val samples = repositoryHandle.ListResultDSSamples(ds + "/exp/", this.Username)

//          samples.asScala foreach println _

          val sch = repositoryHandle.getSchema(ds+ "/exp/", this.Username)

          repositoryHandle.createDs(new IRDataSet(ds, sch), this.Username, samples, scriptPath,if(outputFormat.equals("gtf"))GMQLSchemaTypes.GTF else GMQLSchemaTypes.Delimited)

        }
        elapsedTime.createDsTime = (System.currentTimeMillis() - dstimestamp).toString
        logger.info("DataSet creation Time: " + elapsedTime.createDsTime)

        this.status = Status.SUCCESS
      } catch {
        case ex: GMQLNotValidDatasetNameException => status = Status.DS_CREATION_FAILED; logger.error("The Dataset name is not valid...");ex.printStackTrace(); throw ex
        case ex: Exception => this.status = Status.DS_CREATION_FAILED; logError("Input Dataset is empty , (error in process or Wrong input Selection query).");  ex.printStackTrace(); throw ex
        case ex: Throwable => this.status = Status.DS_CREATION_FAILED; logError("Throwable error ");  ex.printStackTrace(); throw ex
      }
    }
    else
      throw new RuntimeException("The code is not compiled..")

  }

  def logInfo(message: String) = {
    logger.info(message)
    jobOutputMessages.append(message)
  }

  def logError(message: String) = {
    logger.error(message)
    jobOutputMessages.append(message)
  }

  def getExecutionTime(): java.lang.String = if (elapsedTime.executionTime == "") "Execution Under Progress" else elapsedTime.executionTime + " Seconds"
  def getCompileTime(): java.lang.String = if (elapsedTime.compileTime == "") "Compiling" else elapsedTime.compileTime + " Seconds"
  def getDSCreationTime(): java.lang.String = if (elapsedTime.createDsTime == "") "Creating Data set in the repository." else elapsedTime.createDsTime + " Seconds"
  def getMessage() = jobOutputMessages.toString()

  def getJobStatus: Status.Value = this.synchronized {
    this.status
  }

  def getExecJobStatus: Status.Value = this.synchronized {
    if(submitHandle != null)
      {
        this.submitHandle.getStatus()
//        println("\n\n\n\nThe exitCode Is: \n"+state+"\n\n\n\n\n\n\n")
      }
      else Status.PENDING
//    state
  }

//  def getLauncherStatus = this.synchronized {
//    println("\n\n\n\nThe Thread Status Is: \n"+this.submitHandle.getStatus()+"\n\n\n\n\n\n\n")
//    this.submitHandle.getStatus()
//  }

//  def setJobStatus (status:Status.Value)= this.synchronized {
//    this.status = status;
//  }

  def generateJobId(username: String, queryname: String): String = {
//    logger.info(username)
    "job_" + queryname.toLowerCase() + "_" + username + "_" + date
  }

}
