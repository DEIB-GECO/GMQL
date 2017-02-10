package it.polimi.genomics.manager.Launchers

import it.polimi.genomics.core.DataStructures.IRDataSet
import it.polimi.genomics.core.ParsingType.PARSING_TYPE
import it.polimi.genomics.manager.{GMQLJob, Status}
import it.polimi.genomics.repository.{Utilities => General_Utilities}
import it.polimi.genomics.repository.FSRepository.{LFSRepository, FS_Utilities => FSR_Utilities}
import it.polimi.genomics.wsc.Livy.StandAloneWSAPI
import org.slf4j.LoggerFactory
import play.api.libs.json.{JsArray, JsNumber, JsObject, JsString}
import play.api.libs.ws.WSAPI

import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Await
import scala.concurrent.Future

/**
  * Created by abdulrahman on 26/05/16.
  */
class GMQLLivyLauncher (livyJob:GMQLJob)  extends GMQLLauncher(livyJob){

  private final val logger = LoggerFactory.getLogger(this.getClass);
  val baseUrl = "http://biginsights.pico.cineca.it:8998/batches"
  var livyJobID:Option[String] = None

  //Logging to GMQL job log file
  val loggerFile = General_Utilities().getLogDir() + livyJob.jobId.toLowerCase() + ".log"

  val standaloneWSAPI = new StandAloneWSAPI()
  val statusWSAPI = new StandAloneWSAPI()
  // Standard Play-style WSAPI
  val wsAPI: WSAPI = standaloneWSAPI
  val statuswsAPI: WSAPI = statusWSAPI

  //Json Objedct with the configurations of GMQL Job, this will call GMQLExecuteCommand main function,
  //located in GMQL-CLI module
  val jsData = JsObject(Seq(
    "proxyUser" -> JsString("akaitoua"),
    "numExecutors" -> JsNumber(15),   //YARN only
    "executorCores" -> JsNumber(15),
    "executorMemory" -> JsString("3G"),
    "driverCores" -> JsNumber(3),
    "driverMemory" -> JsString("8G"),
    "file"->JsString("/user/akaitoua/GMQL-Cli-2.0-jar-with-dependencies.jar"),
    "className" ->JsString("it.polimi.genomics.cli.GMQLExecuteCommand"),
    "args" -> JsArray(Seq(JsString("-script"), JsString(job.script.script),
      JsString("-scriptpath"), JsString(job.script.scriptPath),
      JsString("-inputs"), JsString(job.inputDataSets.map(x => x._1+":::"+x._2+"/").mkString(",")),
      JsString("-schema"), JsString(job.inputDataSets.map(x => x._2+":::"+getSchema(job,x._1)).mkString(",")),
      JsString("-jobid"), JsString(job.jobId),
      JsString("-outputFormat"),JsString(job.gMQLContext.outputFormat.toString),
      JsString("-username"), JsString(job.username)
    ))
  ))

  logger.info(jsData.toString())

  // handle to the webservice
  private def submitHolder = {
    wsAPI.url(baseUrl)
      .withHeaders("Accept" -> "application/json")
      .withRequestTimeout(10000)
  }

  // The call for the web services is managed by the asyncronous call
  private def submitGMQLJob: Future[Seq[String]] = {
    submitHolder.post(jsData).map(x=>x.json \\ "id" map (_.toString()))
  }

  // extract the schema from the repository
  private def getSchema(job:GMQLJob,DS:String):String = {
    import scala.io.Source
    val repository = new LFSRepository()
    import scala.collection.JavaConverters._
    val ds = new IRDataSet(DS, List[(String,PARSING_TYPE)]().asJava)
    val user = if(repository.DSExistsInPublic(ds))"public" else job.username
    Source.fromFile(General_Utilities().getSchemaDir(user)+DS+".schema").getLines().mkString("")
  }

  //Run GMQLJob remotely
  def run(): GMQLLivyLauncher = {
    try {
      livyJobID = Some(Await.result(submitGMQLJob, 10.seconds) (0) /*take 10 foreach println*/ )
      println("Livy Job ID : " + livyJobID.get)
    } finally {
      // required, else there'll be threads hanging around
      // you might not care to do this though.
      standaloneWSAPI.close()
    }

    this
  }


  /**
    *
    * Return the status of GMQLJob running remotely
    * The status is requested using web service call to Livy web server
    *
    *     */
  def getStatus(): Status.Value={

    def statusHolder = {
      statuswsAPI.url(baseUrl+"/"+livyJobID.get)
        .withHeaders("Accept" -> "application/json")
        .withRequestTimeout(10000)
    }

    val state =  if(livyJobID.isDefined)
    Await.result(statusHolder.get().map(x=>x.json \\ "state" map (_.toString())), 2.seconds)(0)
    else "\"pending\""


    def logHolder = {
      statuswsAPI.url(baseUrl+"/"+livyJobID.get+"/log")
        .withHeaders("Accept" -> "application/json")
        .withRequestTimeout(10000)
    }

    if(livyJobID.isDefined) {
      val log = Await.result(logHolder.get().map(x => x.json \\ "log" map (_.toString().tail.init)), 2.seconds)(0)
      import java.io._
      val pw = new PrintWriter(new File(loggerFile ))
      pw.write(log.replace("\",\"","\n"))
      pw.close
    }

    //TODO extract the log of Knox execution and write it to GMQL job log file

    stateAdapter(state)
  }

  /**
    *
    * Map the status recieved from Livy to GMQLJob Status
    *
    * @param state String from Livy
    * @return {@link Status} of GMQL job
    */
  private def stateAdapter(state:String): Status.Value ={
    state match {
      case "\"pending\"" => Status.PENDING
      case "\"running\"" => Status.RUNNING
      case "\"success\"" => Status.EXEC_SUCCESS
      case "\"starting\"" => Status.PENDING
      case _ => Status.EXEC_FAILED
    }
  }

  /**
    *
    * Not implemented yet
    * Should be a system call to get the application name from remote Livy
    *
    * @return String of the application name
    */
  def getAppName (): String =
  {
    ""
  }

  /**
    *
    * Kill GMQL job, by calling a remote web service of Livy with delete command
    *
    */
  override def killJob() = {
    def deleteHolder = {
      wsAPI.url(baseUrl+"/"+livyJobID.get)
        .withHeaders("Accept" -> "application/json")
        .withRequestTimeout(10000)
    }

    deleteHolder.delete()
  }
}
