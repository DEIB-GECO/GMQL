package it.polimi.genomics.manager

import java.io._
import java.nio.file.Paths

import it.polimi.genomics.compiler._
import it.polimi.genomics.GMQLServer.{GmqlServer, Implementation}
import it.polimi.genomics.core.DataStructures.{IRDataSet, IRVariable}
import it.polimi.genomics.manager.Launchers.{GMQLLauncher, GMQLLocalLauncher}
import it.polimi.genomics.repository.FSRepository.{DFSRepository, FS_Utilities => FSR_Utilities}
import it.polimi.genomics.repository.{GMQLRepository, GMQLSchemaTypes}
import it.polimi.genomics.repository.{Utilities => General_Utilities}
import it.polimi.genomics.repository.GMQLExceptions.GMQLNotValidDatasetNameException
import org.slf4j.LoggerFactory
import java.util.Date
import java.text.SimpleDateFormat

import scala.collection.JavaConverters._
import Status._
import it.polimi.genomics.core.{GMQLOutputFormat, GMQLScript}
import it.polimi.genomics.core.ParsingType.PARSING_TYPE
import org.apache.hadoop.fs.FileSystem

import scala.collection.immutable.HashMap
import scala.collection.mutable
import scala.xml.XML

/**
 * Created by Abdulrahman Kaitoua on 10/09/15.
 * Email: abdulrahman.kaitoua@polimi.it
 *
 */
class GMQLJob(val gMQLContext: GMQLContext, val script:GMQLScript, val username:String) {


  val date = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
  def jobId: String = generateJobId(username, new java.io.File(script.scriptPath).getName.substring(0, new java.io.File(script.scriptPath).getName.indexOf(".")))
  private final val logger = LoggerFactory.getLogger(this.getClass);
  val jobOutputMessages = new StringBuilder

//  var script = fixFileFormat(scriptPath)
//  val regionsURL = if (General_Utilities().MODE == FS_Utilities.HDFS) FS_Utilities.getInstance().HDFSRepoDir else FS_Utilities.getInstance().RepoDir
  case class ElapsedTime (var compileTime:String,var executionTime:String,var createDsTime:String =" Included in The execution time. (details in the log File)")
  val elapsedTime =ElapsedTime("","","")

  var status = PENDING
  var outputVariablesList = List[String]()
  val server = new GmqlServer(gMQLContext.implementation, Some(gMQLContext.binSize.Map))
  val translator = new Translator(server, "")
  var operators: List[Operator] = List[Operator]()
  var inputDataSets:Map[String, String] = new HashMap;
  var submitHandle: GMQLLauncher = null
  var repositoryHandle: GMQLRepository = gMQLContext.gMQLRepository
  var DAG: mutable.MutableList[IRVariable] = mutable.MutableList[IRVariable]()

  /**
    *
    * @param id
    * @return
    */
  def compile(id:String = jobId): (String, List[String]) = {
    status = Status.COMPILING
    General_Utilities().USERNAME = username

    val Compiletimestamp = System.currentTimeMillis();
    try {
      //compile the GMQL Code phase 1
      val languageParserOperators = translator.phase1(script.script)

      //Get the output Datasets names.
      outputVariablesList = languageParserOperators.flatMap(x => x match {
        case d: MaterializeOperator =>
          if (!d.store_path.isEmpty)
            Some(id + "_" + d.store_path.replace("/", "_"))
          else Some(id)
        case _ => None
      })

      // Get the input Dataset names from the Select Statements
      inputDataSets = languageParserOperators.flatMap { x =>
        x match {
          case select: SelectOperator => logger.info(select.op_pos + "\t" + select.output + "\t" + select.parameters);
            val DSname: String = select.input1 match {
              case p: VariablePath => p.path
              case p: Variable => p.name
            }
            val ds = new IRDataSet(DSname, List[(String, PARSING_TYPE)]().asJava)
            if (repositoryHandle.DSExists(ds, username)) {
              val user = if (repositoryHandle.DSExistsInPublic(ds)) "public" else this.username
              Some(DSname, getHDFSRegionFolder(DSname, user))
            } else {
              logger.warn(DSname + " is not a dataset in the repository...error");
              None
            }
          case s: Operator => None
        }
      }.toMap


      val fsRegDir = FSR_Utilities.gethdfsConfiguration().get("fs.defaultFS")+
                    General_Utilities().getHDFSRegionDir()
      //extract the materialize operators, change the Store path to be $JobID_$StorePath
      operators = languageParserOperators.map(x => x match {
        case select: SelectOperator => logger.info(select.op_pos + "\t" + select.output + "\t" + select.parameters);
          val dsinput = select.input1 match {
            case p: VariablePath => val ds = new IRDataSet( p.path, List[(String, PARSING_TYPE)]().asJava)
              if (repositoryHandle.DSExists(ds, username)) {
                val user = if (repositoryHandle.DSExistsInPublic(ds)) "public" else this.username
                val newPath = getHDFSRegionFolder(ds.position, user)
                println(newPath)
                new VariablePath(newPath, p.parser_name);
              } else {
                p
              }
            case p: VariableIdentifier => {
              val ds = new IRDataSet(p.IDName, List[(String, PARSING_TYPE)]().asJava)
              if (repositoryHandle.DSExists(ds, username)) {
                val user = if (repositoryHandle.DSExistsInPublic(ds)) "public" else this.username
                val newPath = getHDFSRegionFolder(ds.position, user)
                println(newPath)
                new VariableIdentifier(newPath);
              } else {
                p
              }
            }
          }
          new SelectOperator(select.op_pos, dsinput, select.input2,select.output,select.parameters)
          /*val ds = new IRDataSet(DSname, List[(String, PARSING_TYPE)]().asJava)
          if (repositoryHandle.DSExists(ds, username)) {
            val user = if (repositoryHandle.DSExistsInPublic(ds)) "public" else this.username
            Some(DSname, getHDFSRegionFolder(DSname, user))
          } else {
            logger.warn(DSname + " is not a dataset in the repository...error");
            None
          }*/
        case d: MaterializeOperator =>
          if (!d.store_path.isEmpty){
            if (General_Utilities().MODE == General_Utilities().HDFS)
                         d.store_path = fsRegDir + id + "_" + d.store_path + "/"
            else d.store_path = id + "_" + d.store_path + "/"
            d
          }
          else{
            if (General_Utilities().MODE == General_Utilities().HDFS)
              d.store_path = fsRegDir + id + "/"
            else d.store_path = id + "/"
            d
          }
        case s: Operator => s
      })

      //Perform the second compilation phase
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

  def getHDFSRegionFolder(dsName:String,user:String): String ={
    val xmlFile = General_Utilities().getDataSetsDir(user) + dsName + ".xml"
   val xml = XML.loadFile(xmlFile)
    val path = (xml \\ "url")(0).text
    if(path.startsWith("hdfs"))
      (new org.apache.hadoop.fs.Path(path)).getParent.toString
    else
      FSR_Utilities.gethdfsConfiguration().get("fs.defaultFS") +
        General_Utilities().getHDFSRegionDir(user)+Paths.get(path).getParent.toString
  }

  def runGMQL(id:String = jobId,submitHand: GMQLLauncher= new GMQLLocalLauncher(this)):Status.Value = {
    this.status = Status.RUNNING

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
          println(jobId+"\t"+status)
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

          val samples = repositoryHandle.ListResultDSSamples(ds + "/exp/", this.username)

          println("samples")
          samples._1.asScala foreach println _

          val sch = samples._2
//          val sch = repositoryHandle.getSchema(ds+ "/exp/", this.username)

          repositoryHandle.createDs(new IRDataSet(ds, sch),
            this.username, samples._1, script.scriptPath,
            if(gMQLContext.outputFormat.equals(GMQLOutputFormat.GTF))GMQLSchemaTypes.GTF else GMQLSchemaTypes.Delimited)

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
      }
      else Status.PENDING
  }


  def generateJobId(username: String, queryname: String): String = {
    "job_" + queryname.toLowerCase() + "_" + username + "_" + date
  }

}
