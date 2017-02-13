package it.polimi.genomics.repository.FSRepository

//import groovy.json.JsonSlurper
import java.io.{File, FileOutputStream}
import java.nio.file.Files

import it.polimi.genomics.core.DataStructures.IRDataSet
import it.polimi.genomics.core.ParsingType
import it.polimi.genomics.core.ParsingType._
import it.polimi.genomics.repository.GMQLExceptions.GMQLUserNotFound
import it.polimi.genomics.repository.{GMQLRepository, GMQLSample}
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.iteratee.Enumerator
import play.api.libs.json._
import play.api.libs.ws.{WSAPI, WSAuthScheme, WSResponseHeaders}

import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Await


/**
  * Created by abdulrahman on 26/05/16.
  */
object testRepository {
  private final val logger = LoggerFactory.getLogger("testRepository")
  val uri1 = new File("GMQL-Repository/src/main/resources/ann/file1.bed").getAbsolutePath
  val uri2 = new File("GMQL-Repository/src/main/resources/ann/file2.bed").getAbsolutePath
  val S1 = new GMQLSample(uri1,uri1+".meta")
  val S2 = new GMQLSample(uri2,uri2+".meta")
  val sch = List[(String,PARSING_TYPE)](("name",ParsingType.STRING),("score",ParsingType.DOUBLE))
  import scala.collection.JavaConverters._
  var DSname = "LFStest113"
  def irDS = new IRDataSet(DSname,sch.asJava)
  var username = "abdulrahman"
  var REPO:GMQLRepository = new LFSRepository()

  def main(args: Array[String]) {

    for (i <- 0 until args.length if (i % 2 == 0)) {
      if ("-h".equals(args(i)) || "-help".equals(args(i))) {
        println("This program is ment to test the Repository...")
      }
      else if ("-user".equals(args(i))) {
        username = args(i + 1).toLowerCase()

        logger.info(s"username : $username")
      } else if ("-repo".equals(args(i))) {
        val rep = args(i + 1).toUpperCase()
        if (rep == "LOCAL")
          REPO = new LFSRepository()
        else if (rep == "HDFS")
          REPO = new DFSRepository()
        else REPO = new RFSRepository()
        logger.info(s"Repository set to: $rep")
      } else if ("-dsname".equals(args(i))) {
        DSname = args(i + 1)
        logger.info(s"DataSet name is set to: $DSname")
      }
    }

    println(FS_Utilities.gethdfsConfiguration().get("fs.defaultFS"))

        testCreateDS();
//        REPO.exportDsToLocal(irDS.position,username,"/Users/abdulrahman/Downloads/ddd/")
//    REPO.ListAllDSs("abdulrahman").asScala.foreach(x=>println(x.position))
//    REPO.DeleteDS(irDS.position,username)

//    try
//      {
//          println(REPO.DSExists(irDS,username))
//      }catch
//      {
//        case ex:GMQLUserNotFound => println("user not found")
//      }
    //ds exist
    //ds exists in public
    //delete sample
    //delete ds

  }

  def testCreateDS() ={

    val samples = List[GMQLSample](S1, S2).asJava
    println(S1.name,S2.name)
    println(username)
    REPO.createDs(irDS,username,samples)
  }

  def testGetMeta()={
       println( REPO.getSampleMeta(irDS,username,S1))
       println("\n//////////////////////////////\n\n//////////////////////////////")
       println( new LFSRepository().getMeta(irDS,username))
  }

}
