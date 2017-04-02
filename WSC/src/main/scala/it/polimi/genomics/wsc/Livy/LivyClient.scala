package it.polimi.genomics.wsc.Livy

import it.polimi.genomics.manager.{Status, Utilities}
import it.polimi.genomics.wsc.Knox.LooseWSAPI
import play.api.libs.ws.WSAuthScheme

import scala.util.Random
import scala.concurrent.duration._
import scala.concurrent.Await
import play.api.libs.json._

/**
  * Created by abdulrahman on 25/05/16.
  * Updated by andreagulino on 10/03/17
  */

object InvyClient {

  val WSAPI_Livy = new StandAloneWSAPI()
  val WSAPI_Knox = new LooseWSAPI()

  // Knox Authentication
  val kUser = Utilities().KNOX_USERNAME
  val kPass = Utilities().KNOX_PASSWORD

  // Livy Base URL
  val livyBaseUrl = Utilities().LIVY_BASE_URL

  val ran = "job_" + Random.nextInt(100000)



  def main(args: Array[String]): Unit = {


    if( args.isEmpty ) {
      println("args0:"+
        "\t 1 - Get job status (job id in arg1) \n" +
        "\t 2 - Perform query (query script in arg1)  \n" +
        "\t 3 - Get application id and log for livy job id (arg1)")
      return
    }



    try {
      if (args.size > 1 && args(0) == "1") {

        // Get Status
        val status = getStatus(args(1))
        // Check If succeed to get the status
        if( status.isDefined ) {
          println(status.get)
        } else {
          println("Cannot get the status of the application (maybe is too old)")
        }



      } else if(args.size > 1 && args(0) == "2") {

        // Query
        println(query(args(1)))


      } else if(args.size > 1 && args(0) == "3") {

        val status = getStatus(args(1))

        // Check If succeed to get the status
        if( status.isDefined ) {

          // Check if execution succeeded
          if(status.get.equals(Status.EXEC_SUCCESS)) {

            // Get app name
            val app_name = getAppName(args(1))

            // Check if app name is defined
            if (app_name.isDefined) {
              println(getLog(app_name.get))
            } else {
              println("Failed to get the app name.")
            }
          } else {
            println("Cannot get the log while the job is still running")
          } // end check exec succ.
        } else {
          println("Cannot get the status of the application (maybe is too old)")
        }

      } else {
        println("No operation defined for the provided args.")
      }

    } finally {
      // required, else there'll be threads hanging around
      // you might not care to do this though.
      WSAPI_Livy.close()
      WSAPI_Knox.close()
    }
  }


  def getStatus(livyJobId:String): Option[Status.Value] = {
    def statusHolder = {
      WSAPI_Livy.url(livyBaseUrl+"/"+livyJobId)
        .withHeaders("Accept" -> "application/json")
        .withRequestTimeout(10000)
    }

    val result = Await.result(statusHolder.get(), 2.seconds)
    if( result.status == 404 ) {
      None
    } else {
      val state =  (result.json \\ "state")(0).toString()
      print(" \n\n\n### STATE IS "+state+" ####\n\n\n")
      Some(stateAdapter(state))
    }




  }

  private def query(script: String): String = {

    def holder = {
      WSAPI_Livy.url(livyBaseUrl)
        .withHeaders("Accept"       -> "application/json")
        .withHeaders("Content-Type" -> "application/json")
        .withRequestTimeout(10000)
    }

    val jsData = JsObject(Seq(
      "proxyUser"      -> JsString(Utilities().KNOX_USERNAME),
      "name"           -> JsString("GMQL Livy Test"),
      /*"numExecutors"   -> JsNumber(15),
      "executorCores"  -> JsNumber(15),
      "executorMemory" -> JsString("3G"),
      "driverCores"    -> JsNumber(3),
      "driverMemory"   -> JsString("8G"), */
      "file"           -> JsString(Utilities().REMOTE_CLI_JAR_PATH),
      "className"      -> JsString(Utilities().CLI_CLASS),
      "args"           -> JsArray(Seq(
        JsString("-script"), JsString(script),
        JsString("-scriptpath"), JsString("test.gmql"),
        JsString("-jobid"), JsString(ran),
        JsString("-username"), JsString("abdo")
      ))
    ))



    (Await.result(holder.post(jsData), 10.seconds).json \\ "id")   (0) toString()

  }


  def getAppName(livyJobId:String): Option[String] = {

    println("Getting App Name")

    def holder = {
      WSAPI_Livy.url(livyBaseUrl + "/" + livyJobId + "/log")
        .withHeaders("Accept"       -> "application/json")
        .withHeaders("Content-Type" -> "application/json")
        .withRequestTimeout(10000)
    }


    val res =  Await.result(holder.get(), 2.seconds)

    println("RESPONSE STATUS CODE: "+res.status)

    if( res.status == 404 ) {
      None
    }  else {

      val log = res.json \\ "log" toString()
      val pattern_an = "(application_[0-9]+_[0-9]+)".r
      pattern_an.findFirstIn(log)

    }

  }


  def getLog(appName:String) : String = {

    val attempts = Array("01", "02", "03", "04", "05", "06")


    for( attempt <- attempts ) {

      var app_id = appName.replaceAll("application_", "")

      val a = WSAPI_Knox.url(getLogURL(app_id, attempt))
        .withRequestTimeout(10000)
        .withAuth(kUser, kPass, WSAuthScheme.BASIC)
        .withHeaders("Accept"       -> "application/json")
        .withHeaders("Content-Type" -> "application/json")



      var log_res =  Await.result(a.get(), 2.seconds)

      val pattern_log = "<pre>.*</pre>".r

      println("\n\n\nBody:")
      println(log_res.body)

      val escaped = log_res.body.toString
        .replaceAll("\n", "GECONEWLINE")
        .replaceAll("\r", "GECOCARRIAGE")
        .replaceAll("\t", "GECOTAB")
        .replaceAll("\b", "GECOBACKSLASH")
        .replaceAll("\f", "GECOFORMFEED")


      println("\n\n\nEscaped:")
      println(escaped)

      val matched_log = pattern_log.findFirstIn(escaped)

      if( !matched_log.isEmpty ) {
        return matched_log.get.replace("<pre>", "").replace("</pre>","")
          .replaceAll("GECONEWLINE"  , "\n")
          .replaceAll("GECOCARRIAGE" , "\r")
          .replaceAll("GECOTAB"      , "\t")
          .replaceAll("GECOBACKSLASH", "\b")
          .replaceAll("GECOFORMFEED" , "\f")
      } else {
        println("Attempt URL ("+getLogURL(app_id, attempt)+") failed.")

      }

    }

    "Cannot retrieve the Log."


  }

  private def getLogURL(appId: String, hostNumber: String ): String = {

    "https://biginsights.pico.cineca.it:8443/gateway/"+
      "jobstoryui/jobstory/jobhistory/logs/bi"+
      hostNumber +
      ".pico.cineca.it:45454/"+
      "container_"+appId+"_01_000001"+"/"+
      "container_"+appId+"_01_000001"+"/"+
      "/akaitoua/stderr?start=0"
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
