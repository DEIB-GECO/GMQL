package it.polimi.genomics.manager.Launchers

import it.polimi.genomics.manager.{Status, GMQLJob}
import it.polimi.genomics.repository.util.Utilities
import it.polimi.genomics.wsc.Livy.StandAloneWSAPI
import org.slf4j.LoggerFactory
import play.api.libs.json.{JsNumber, JsArray, JsString, JsObject}
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

  val loggerFile = Utilities.getInstance().GMQLHOME + "/data/" + Utilities.USERNAME + "/logs/" + livyJob.jobId.toLowerCase() + ".log"

  val standaloneWSAPI = new StandAloneWSAPI()
  val statusWSAPI = new StandAloneWSAPI()
  // Standard Play-style WSAPI
  val wsAPI: WSAPI = standaloneWSAPI
  val statuswsAPI: WSAPI = statusWSAPI

  val jsData = JsObject(Seq(
    "proxyUser" -> JsString("akaitoua"),
    "numExecutors" -> JsNumber(15),   //YARN only
    "executorCores" -> JsNumber(15),
    "executorMemory" -> JsString("3G"),
    "driverCores" -> JsNumber(3),
    "driverMemory" -> JsString("8G"),
    "file"->JsString("/user/akaitoua/GMQL-Cli-2.0-jar-with-dependencies.jar"),
    "className" ->JsString("it.polimi.genomics.cli.GMQLExecuteCommand"),
    "args" -> JsArray(Seq(JsString("-script"), JsString(job.script),
      JsString("-scriptpath"), JsString(job.scriptPath),
      JsString("-inputs"), JsString(job.inputDataSets.map(x => x._1+":::"+x._2+"/").mkString(",")),
      JsString("-schema"), JsString(job.inputDataSets.map(x => x._2+":::"+getSchema(job,x._1)).mkString(",")),
      JsString("-jobid"), JsString(job.jobId),
      JsString("-outputFormat"),JsString(job.outputFormat),
      JsString("-username"), JsString(job.username)
    ))
  ))

  logger.info(jsData.toString())

  def submitHolder = {
    wsAPI.url(baseUrl)
      .withHeaders("Accept" -> "application/json")
      .withRequestTimeout(10000)
  }

  def submitGMQLJob: Future[Seq[String]] = {
    submitHolder.post(jsData).map(x=>x.json \\ "id" map (_.toString()))
  }

  def getSchema(job:GMQLJob,DS:String):String = {
    import scala.io.Source
    import it.polimi.genomics.repository.util.Utilities;
    val user = if(Utilities.getInstance().checkDSNameinPublic(DS))"public" else job.username
    Source.fromFile(Utilities.getInstance().RepoDir+user+"/schema/"+DS+".schema").getLines().mkString("")
  }

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

    stateAdapter(state)
  }

  def stateAdapter(state:String): Status.Value ={
    state match {
      case "\"pending\"" => Status.PENDING
      case "\"running\"" => Status.RUNNING
      case "\"success\"" => Status.EXEC_SUCCESS
      case "\"starting\"" => Status.PENDING
      case _ => Status.EXEC_FAILED
    }
  }

  def getAppName (): String =
  {
    ""
  }

  override def killJob() = {
    def deleteHolder = {
      wsAPI.url(baseUrl+"/"+livyJobID.get)
        .withHeaders("Accept" -> "application/json")
        .withRequestTimeout(10000)
    }

    deleteHolder.delete()
  }
}
