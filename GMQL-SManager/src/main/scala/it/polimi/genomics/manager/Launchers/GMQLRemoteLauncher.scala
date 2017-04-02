package it.polimi.genomics.manager.Launchers

import java.io.{BufferedWriter, File, FileWriter}

import it.polimi.genomics.manager.{GMQLJob, Status, Utilities}
import it.polimi.genomics.repository.{Utilities => General_Utilities}
import it.polimi.genomics.repository.FSRepository.{LFSRepository, FS_Utilities => FSR_Utilities}
import it.polimi.genomics.wsc.Knox.{KnoxClient, KnoxOperation, LooseWSAPI}
import it.polimi.genomics.wsc.Livy.StandAloneWSAPI
import org.apache.commons.io.FileUtils
import org.slf4j.LoggerFactory
import play.api.libs.json.{JsArray, JsNumber, JsObject, JsString}
import play.api.libs.ws.WSAuthScheme

import scala.concurrent.duration._
import scala.concurrent.Await
import scala.concurrent.Future

/**
  * Created by abdulrahman on 26/05/16.
  * Edited by andreagulino on 20/03/17.
  */
class GMQLRemoteLauncher(livyJob:GMQLJob) extends GMQLLauncher(livyJob){


  private final val logger     = LoggerFactory.getLogger(this.getClass);
  private final val loggerFile = General_Utilities().getLogDir(job.username)+job.jobId.toLowerCase()+".log"

  /* Livy Client Config */
  val WSAPI_Livy = new StandAloneWSAPI()
  val livyBaseUrl = Utilities().LIVY_BASE_URL


  /* Knox Client Config */
  val WSAPI_Knox = new LooseWSAPI()
  val kUser = Utilities().KNOX_USERNAME
  val kPass = Utilities().KNOX_PASSWORD

  private def getLogURL(appId: String, hostNumber: String ): String = {

    Utilities().KNOX_GATEWAY+
      "jobstoryui/jobstory/jobhistory/logs/bi"+
      hostNumber +
      ".pico.cineca.it:45454/"+
      "container_"+appId+"_01_000001"+"/"+
      "container_"+appId+"_01_000001"+"/"+
      "/akaitoua/stderr?start=0"
  }

  // Livy ID and Application ID can be retrieved after a job is launched
  var livyJobID:Option[String] = None
  override var applicationID: Option[String] = None




  /*
   Json object to be sent to Livy Server encapsulating both
     - spark-submit parameters
     - GMQL-Cli parameters
   */


  val outDir = job.outputVariablesList.map{x=>
    val dir = General_Utilities().REMOTE_HDFS_HOST +
      General_Utilities().getHDFSRegionDir(job.username)+x+"/";

    x.substring(job.jobId.length+1)+":::"+dir
  }.mkString(",")


  val jsData = JsObject(Seq(
    "proxyUser" -> JsString(Utilities().KNOX_USERNAME),
    "name"      -> JsString(job.jobId),
    /*
    These parameters are commented because not allowed by CINECA

    "numExecutors"   -> JsNumber(15),   //YARN only
    "executorCores"  -> JsNumber(15),
    "executorMemory" -> JsString("3G"),
    "driverCores"    -> JsNumber(3),
    "driverMemory"    -> JsString("8G"),

    */
    "file"      ->JsString(Utilities().REMOTE_CLI_JAR_PATH),
    "className" ->JsString(Utilities().CLI_CLASS),
    "args"      -> JsArray( Seq(
      JsString("-script")      , JsString(job.script.script),
      JsString("-scriptpath")  , JsString(job.script.scriptPath),
      JsString("-inputDirs")   , JsString(job.inputDataSets.map(x => x._1+":::"+x._2+"/").mkString(",")),
      // JsString("-schema"), JsString(job.inputDataSets.map(x => x._2+":::"+getSchema(job,x._1)).mkString(",")),
      JsString("-jobid")       , JsString(job.jobId),
      JsString("-outputDirs")  , JsString(outDir),
      JsString("-outputFormat"),JsString(job.gMQLContext.outputFormat.toString),
      JsString("-username")    , JsString(job.username)
    ))
  ))

  /**
    * Run the job invoking Livy API
    * @return [[ GMQLLauncher]]
    */
  override def run(): GMQLRemoteLauncher = {

    print("Calling livy API with json object:")
    print(jsData.toString())


    def holder = {
      WSAPI_Livy.url(livyBaseUrl)
        .withHeaders("Accept"       -> "application/json")
        .withHeaders("Content-Type" -> "application/json")
        .withRequestTimeout(10000)
    }



    livyJobID = Some( (Await.result(holder.post(jsData), 10.seconds).json \\ "id") (0) toString)

    this
  }

  /**
    * Kill the job calling Livy API  todo: is it good to kill the job in this way?
    */
  override def killJob() = {

    def deleteHolder = {
      WSAPI_Livy.url(livyBaseUrl+"/"+livyJobID.get)
        .withHeaders("Accept" -> "application/json")
        .withRequestTimeout(10000)
    }

    deleteHolder.delete()
  }

  /**
    * Get the status of the query (actually the status of the execution)
    * @return The [[ GMQLJob]] Status [[ Status]]
    */
  override def getStatus(): Status.Value ={

    if( livyJobID.isDefined ) {

      def statusHolder = {
        WSAPI_Livy.url(livyBaseUrl + "/" + livyJobID.get)
          .withHeaders("Accept" -> "application/json")
          .withRequestTimeout(10000)
      }

      val result = Await.result(statusHolder.get(), 2.seconds)

      if (result.status == 404) {
        stateAdapter("") //todo: define a state 'unknown'
      } else {
        val state = (result.json \\ "state") (0).toString()
        stateAdapter(state)
      }
    } else {
      stateAdapter("") //todo: define a state 'unknown'
    }

  }



  /**
    *
    * Extracts the app name by looking for it in the Livy log
    *
    * @return String of the application name or empty String
    */
  override  def getAppName (): String = {

    var appName = ""

    if(livyJobID.isDefined) {

      def holder = {
        WSAPI_Livy.url(livyBaseUrl + "/" + livyJobID + "/log")
          .withHeaders("Accept"       -> "application/json")
          .withHeaders("Content-Type" -> "application/json")
          .withRequestTimeout(10000)
      }


      val log_str =  Await.result(holder.get(), 2.seconds).json \\ "log" toString
      val pattern = "(application_[0-9]+_[0-9]+)".r
      appName  = pattern.findFirstIn(log_str).get

    }

    applicationID = Some(appName)
    applicationID.get
  }

  /**
    * get the log of the execution of GMQL job running using this launcher
    * Notice: this is a workaround that exploits knox gateway to get the html log page of yarn history
    *         log can be retrieved only when the execution is completed.
    *
    * @return List[String] as the log of the execution. An error message if called before end of execution or failed to retrieve.
    */
  override def getLog(): List[String] = {

    val isAppIdDefined   = applicationID.isDefined
    val isExecSuccess    =  getStatus().equals(Status.EXEC_SUCCESS)

    if( !isAppIdDefined || !isExecSuccess ) {
      return "Cannot get the log while the job is still running.".split("\n").toList
    }


    // Since the URL where the log is retrieved can change (and we cannot know it),
    // we do several attempts until we find a web page containing a <pre> tag
    val attempts = Array("01", "02", "03", "04", "05", "06")

    for( attempt <- attempts ) {

      var app_id = applicationID.get.replaceAll("application_", "")

      val a = WSAPI_Knox.url(getLogURL(app_id, attempt))
        .withRequestTimeout(10000)
        .withAuth(kUser, kPass, WSAuthScheme.BASIC)
        .withHeaders("Accept" -> "application/json")
        .withHeaders("Content-Type" -> "application/json")


      var log_res = Await.result(a.get(), 2.seconds)

      // In order to locate the log inside the html we use regex after escaping the string

      val pattern_log = "<pre>.*</pre>".r

      val escaped = log_res.body.toString
        .replaceAll("\n", "GECONEWLINE")
        .replaceAll("\r", "GECOCARRIAGE")
        .replaceAll("\t", "GECOTAB")
        .replaceAll("\b", "GECOBACKSLASH")
        .replaceAll("\f", "GECOFORMFEED")


      val matched_log = pattern_log.findFirstIn(escaped)

      if (!matched_log.isEmpty) {
        val log_string =  matched_log.get.replace("<pre>", "").replace("</pre>", "")
          .replaceAll("GECONEWLINE"  , "\n")
          .replaceAll("GECOCARRIAGE" , "\r")
          .replaceAll("GECOTAB"      , "\t")
          .replaceAll("GECOBACKSLASH", "\b")
          .replaceAll("GECOFORMFEED" , "\f")


        // Write file on HDFS at log dir folder
        val file = new File(job.jobId+".log")
        val bw = new BufferedWriter(new FileWriter(file))
        bw.write(log_string)
        bw.close()
        KnoxClient.uploadFile(loggerFile, job.jobId+".log" )
        FileUtils.deleteQuietly(file)

        return log_string.split("\n").toList
      }

    }

    "Failed retrieving the Log.".split("\n").toList

  }



  /**
    *
    * Map the status recieved from Livy to GMQLJob Status
    *
    * @param state String from Livy
    * @return [[ Status]] of GMQL job
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


}
