package it.polimi.genomics.wsc.Knox

import java.io.{File, InputStream, PipedInputStream, PipedOutputStream}

import play.api.libs.iteratee.{Enumerator, Iteratee}
import play.api.libs.ws.{WSAPI, WSAuthScheme}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import it.polimi.genomics.wsc.WSUtilities

/**
  * Created by abdulrahman on 30/05/16.
  * Updated by andreagulino on 30/03/17
  */
object KnoxClient {


  var standaloneWSAPI = new LooseWSAPI()

  // Standard Play-style WSAPI
  def wsAPI: WSAPI = standaloneWSAPI

  val GATEWAY = WSUtilities().KNOX_GATEWAY
  val SERVICE_PATH = WSUtilities().KNOX_SERVICE_PATH

  val USERNAME = WSUtilities().KNOX_USERNAME
  val PASSWORD = WSUtilities().KNOX_PASSWORD

  def downloadFile(inputfile: String, outputFile: File) = {
    DownloadFile(inputfile, outputFile)
    //val content = Await.result(DownloadFile(inputfile, outputFile), 20.seconds)
    //println(content.getPath, content.exists())
  }

  def mkdirs(inputfile: String) = {
    val holder = Holder(inputfile, KnoxOperation.MKDIRS).withQueryString("permission" -> "755").put(inputfile)
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

  def uploadDir(localFolder: String, remoteFolder: String) : Unit =  {

    val contents = getListOfFiles(localFolder)
    val files = contents flatMap  { x => if (x.isFile ) Some(x.getName) else None }
    val folders = contents flatMap { x => if (x.isDirectory) Some(x.getName) else None }

    contents foreach println _
    for (file <- files) {
      val holder = Holder(remoteFolder + "/" + file, KnoxOperation.UPLOAD).put(new File(localFolder + "/" + file))
      Await.result(holder.map { x => println("upload ", x.toString) }, 3600.second)
    }


    for (folder <- folders)
      this.uploadDir(localFolder+"/"+folder, remoteFolder + "/" +folder)

  }

  def DownloadFile(inputPath: String, outputFile: File): Unit /*Future[File]*/ = {


    val is = downloadAsStream(inputPath)

    val in = scala.io.Source.fromInputStream(is)
    val out = new java.io.PrintWriter(outputFile)
    try { in.getLines().foreach(out.println(_)) }
    finally { out.close }

  }

  def downloadAsStream(inputPath: String): InputStream /*Future[File]*/ = {

    import scala.concurrent.ExecutionContext.Implicits.global

    val holder =
      Holder(inputPath, KnoxOperation.DOWNLOAD)


    val enumeratorF: Future[Enumerator[Array[Byte]]] = holder.getStream().map(_._2)


    val pos = new PipedOutputStream()
    val pis = new PipedInputStream(pos)


    enumeratorF.map{ (enumerator: Enumerator[Array[Byte]]) =>

      val it = Iteratee.foreach { t: Array[Byte] =>
        pos.write(t) }

      enumerator.onDoneEnumerating(pos.close()) |>>>  it
    }

    pis

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
    /*Await.result(*/DownloadFile(remotefolder + "/" + file, new File(localFolder + "/" + file))/*, 10.seconds)*/
  }

  def Holder(inputPath: String, OP: KnoxOperation.Value) = {
    val s = wsAPI.url(GATEWAY + SERVICE_PATH + inputPath)
      .withRequestTimeout(30000)
      .withAuth(USERNAME, PASSWORD, WSAuthScheme.BASIC)
      .withQueryString("op" -> OP.toString)
    println(s)
    s
  }

  def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }
}
