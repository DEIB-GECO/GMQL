package it.polimi.genomics.wsc.Knox

import java.io.{File, FileOutputStream}

import play.api.libs.ws.{WSAPI, WSAuthScheme, WSResponseHeaders}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

/**
  * Created by abdulrahman on 30/05/16.
  */
object KnoxClient {


  var standaloneWSAPI = new LooseWSAPI()

  // Standard Play-style WSAPI
  def wsAPI: WSAPI = standaloneWSAPI

  val gateway = "https://biginsights.pico.cineca.it:8443/gateway/default"
  val runMRGateWay = "https://localhost:8443/gateway/default/templeton/v1/mapreduce/jar"

  val SERVICE_PATH = "/webhdfs/v1/"

  val Authusername = "akaitoua"

  val password = "fsl-hggi11"

  def downloadFile(inputfile: String, outputFile: File) = {
      val content = Await.result(DownloadFile(inputfile, outputFile), 10.seconds)
      println(content.getPath, content.exists())
  }

  def mkdirs(inputfile: String) = {
      val holder = Holder(inputfile, KnoxOperation.MKDIRS).withQueryString("permission" -> "777").put(inputfile)
      Await.result(holder.map { x => println("mkdir ", x.toString) }, 10.second)
  }

  def delDIR(inputfile: String) = {
    val holder = Holder(inputfile, KnoxOperation.DELETE).withQueryString("recursive" -> "true").delete()
    Await.result(holder.map { x => println("delete ", x.toString) }, 10.second)
  }

  def uploadFile(inputfile: String, outputFile: String) = {
      val holder = Holder(outputFile, KnoxOperation.UPLOAD).put(new File(inputfile))
      Await.result(holder.map { x => println("upload ", x.toString) }, 10.second)
  }

  def DownloadFile(inputPath: String, outputFile: File): Future[File] = {

    import play.api.libs.iteratee._

    val futureResponse: Future[(WSResponseHeaders, Enumerator[Array[Byte]])] =
      Holder(inputPath, KnoxOperation.DOWNLOAD).getStream()

    val downloadedFile: Future[java.io.File] = futureResponse.flatMap {
      case (headers, body) =>
        val outputStream = new FileOutputStream(outputFile)

        // The iteratee that writes to the output stream
        val iteratee = Iteratee.foreach[Array[Byte]] { bytes =>
          outputStream.write(bytes)
        }

        // Feed the body into the iteratee
        (body |>>> iteratee).andThen {
          case result =>
            // Close the output stream whether there was an error or not
            outputStream.close()
            // Get the result or rethrow the error
            result.get
        }.map(_ => outputFile)
    }
    downloadedFile
  }

  def listFiles(inputPath: String): Future[Seq[(String, String)]] = {
    val holder = Holder(inputPath, KnoxOperation.LISTSTATUS).get()
    val contents = holder.map { x => val pathes = x.json \\ "pathSuffix" map (_.toString().tail.init); val types = x.json \\ "type" map (_.toString().tail.init); pathes.zip(types) }
    println(contents)
    contents
  }

  def downloadFolder(remotefolder: String, localFolder: String): Unit = {
      val contents = Await.result(listFiles(remotefolder), 10.second)
      val files = contents flatMap { x => if (x._2.equals("FILE")) Some(x._1) else None }
      val folders = contents flatMap { x => if (!x._2.equals("FILE")) Some(x._1) else None }
      contents foreach println _
      for (file <- files)
        Await.result(DownloadFile(remotefolder + "/" + file, new File(localFolder + "/" + file)), 10.seconds)
  }

  def Holder(inputPath: String, OP: KnoxOperation.Value) = {
    val s = wsAPI.url(gateway + SERVICE_PATH + inputPath)
      .withRequestTimeout(10000)
      .withAuth(Authusername, password, WSAuthScheme.BASIC)
      .withQueryString("op" -> OP.toString)
    println(s)
    s
  }
}
