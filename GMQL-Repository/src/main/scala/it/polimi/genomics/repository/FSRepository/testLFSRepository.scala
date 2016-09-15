package it.polimi.genomics.repository.FSRepository

//import groovy.json.JsonSlurper
import java.io.{File, FileOutputStream}
import java.nio.file.Files

import it.polimi.genomics.core.DataStructures.IRDataSet
import it.polimi.genomics.core.ParsingType
import it.polimi.genomics.core.ParsingType._
import it.polimi.genomics.repository.GMQLRepository.GMQLSample
import play.api.libs.iteratee.Enumerator
import play.api.libs.json._
import play.api.libs.ws.{WSResponseHeaders, WSAuthScheme, WSAPI}
import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Await


/**
  * Created by abdulrahman on 26/05/16.
  */
object testLFSRepository {
  val uri1 = "/Users/abdulrahman/Downloads/job_histogram_abdulrahman_20160519_184225_data/S_-2986345875652195707.gtf"
  val uri2 = "/Users/abdulrahman/Downloads/job_histogram_abdulrahman_20160519_184225_data/S_-4536275001140494738.gtf"
  val S1 = new GMQLSample(uri1,uri1+".meta")
  val S2 = new GMQLSample(uri2,uri2+".meta")
  val sch = List[(String,PARSING_TYPE)](("score",ParsingType.DOUBLE))
  import scala.collection.JavaConverters._
  val irDS = new IRDataSet("LFStest11",sch.asJava)
  val username = "abdulrahman"


  def main(args: Array[String]) {


    println(Utilities.gethdfsConfiguration().get("fs.defaultFS"))

//        testCreateDS();
//        new LFSRepository().exportDsToLocal(irDS.position,username,"/Users/abdulrahman/Downloads/ddd/")
    println(new LFSRepository().ListAllDSs("abdulrahman"))
  }

  def testCreateDS() ={

    val samples = List[GMQLSample](S1, S2).asJava
    new LFSRepository().createDs(irDS,username,samples)
  }
}
