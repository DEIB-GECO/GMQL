package it.polimi.genomics.wsc.Livy

import play.api.libs.ws.WSAPI

import scala.concurrent.Await
import scala.util.Random
import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Await

//    import com.fasterxml.jackson.databind.JsonNode;
import play.api.libs.json._

/**
  * Created by abdulrahman on 25/05/16.
  */

object InvyClient {
  def main(args: Array[String]): Unit = {
    val standaloneWSAPI = new StandAloneWSAPI()

    // Standard Play-style WSAPI
    val wsAPI: WSAPI = standaloneWSAPI
    val ran = "job_" + Random.nextInt(100000)
    val schema = "/user/akaitoua/gmql_repo//public/regions/home/pinoli/importer/Encode_import/hg_narrowPeaks:::<?xml version=\"1.0\" encoding=\"UTF-8\"?><gmqlSchemaCollection name=\"GLOBAL_SCHEMAS\" xmlns=\"http://genomic.elet.polimi.it/entities\"><gmqlSchema  type=\"narrowPeak\"><field type=\"STRING\">CHR</field><field type=\"LONG\">start</field><field type=\"LONG\">stop</field><field type=\"STRING\">name</field><field type=\"INTEGER\">score</field><field type=\"CHAR\">STRAND</field><field type=\"FLOAT\">signal</field><field type=\"FLOAT\">pvalue</field><field type=\"FLOAT\">qvalue</field><field type=\"INTEGER\">peak</field></gmqlSchema></gmqlSchemaCollection>"

    val baseUrl = "http://biginsights.pico.cineca.it:8998/batches"

    val livyJobID = if (args.size > 1) Some(args(1)) else None

    try {
      if (args(0) == "state") {
        def statusHolder = {
         val ss = wsAPI.url(baseUrl + "/" + livyJobID.get)
            .withHeaders("Accept" -> "application/json")
            .withRequestTimeout(10000)
          println(ss)
          ss
        }

        val state = /*if (livyJobID.isDefined)*/
          Await.result(statusHolder.get().map(x => x.json \\ "state" map (_.toString())), 2.seconds)(0)
//        else "pending"

        println("State is : ",state)

      } else {
        def holder = {
          wsAPI.url(baseUrl)
            .withHeaders("Accept" -> "application/json")
            .withRequestTimeout(10000)
        }
        //    val json = Json.parse(
        //      """
        //         {
        //          "file" : "/user/akaitoua/GMQL-Cli-2.0-jar-with-dependencies.jar",
        //          "className" : "it.polimi.genomics.cli.GMQLExecuteCommand"
        //         }
        //      """)

        val jsData = JsObject(Seq(
          "proxyUser" -> JsString("akaitoua"),
          "numExecutors" -> JsNumber(15),   //YARN only
          "executorCores" -> JsNumber(15),
          "executorMemory" -> JsString("3G"),
          "driverCores" -> JsNumber(3),
          "driverMemory" -> JsString("8G"),
          "file" -> JsString("/user/akaitoua/GMQL-Cli-2.0-jar-with-dependencies.jar"),
          "className" -> JsString("it.polimi.genomics.cli.GMQLExecuteCommand"),
          "args" -> JsArray(Seq(JsString("-script"), JsString("""DATA = SELECT( antibody == 'Bach1_(sc-14700)' ) HG19_ENCODE_NARROW; THETOP = HISTOGRAM(1,ANY) DATA; MATERIALIZE DATA into data; MATERIALIZE THETOP into res;"""),
            JsString("-scriptpath"), JsString("/data/gmql_repo/data/abdulrahman/queries/histogram.gmql"),
            JsString("-inputDirs"), JsString("HG19_ENCODE_NARROW:::/user/akaitoua/gmql_repo//public/regions/home/pinoli/importer/Encode_import/hg_narrowPeaks/"),
            JsString("-schemata"), JsString(schema),
            JsString("-jobid"), JsString(ran),
            JsString("-username"), JsString("abdulrahman"),
            JsString("-outputFormat"), JsString("gtf")
          ))
        ))

        println(jsData)
        def getPlanetScalaTitles: Future[ /*Seq[*/ String /*]*/ ] = {
          holder.post(jsData //,
          ).map(x => x.json.toString() /*\\ "state" map (_.toString())*/)
        }


        Await.result(getPlanetScalaTitles, 10.seconds) /*take 10*/ foreach println


      }
    } finally {
      // required, else there'll be threads hanging around
      // you might not care to do this though.
      standaloneWSAPI.close()
    }
  }
}
