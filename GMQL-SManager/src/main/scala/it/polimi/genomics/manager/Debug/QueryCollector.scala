package it.polimi.genomics.manager.Debug

import java.io.{BufferedWriter, File, FileWriter}

import it.polimi.genomics.repository.{Utilities => RepoUtils}
import org.apache.log4j.{Level, Logger}
import org.slf4j
import org.slf4j.LoggerFactory

import scala.collection.mutable.MutableList
import scala.io.Source

object QueryCollector {

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  val logger: slf4j.Logger = LoggerFactory.getLogger(QueryCollector.getClass)


  private final val usage: String = "MakeDebug " +
    " -conf GMQL_REPO "
  " -out DIR"


  def main(args: Array[String]): Unit = {

    var confFolder =  "/Users/andreagulino/Projects/GMQL-WEB/conf/gmql_conf/"
    var outDir = "/Users/andreagulino/Desktop/"

    // Read Options
    for (i <- args.indices if i % 2 == 0) {
      if ("-h".equals(args(i)) || "-help".equals(args(i))) {
        println(usage)
        System.exit(0)

      } else if ("-conf".equals(args(i))) {
        confFolder = args(i + 1)
        logger.info("-conf: " + confFolder)
      } else if ("-out".equals(args(i))) {
        outDir = args(i + 1)
        logger.info("-out: " + outDir)
      } else {
        logger.warn(s"Command option is not found ${args(i)}")
        System.exit(0)
      }
    }

    RepoUtils.confFolder = confFolder

    val repository = RepoUtils().getRepository()
    val repoDir = RepoUtils().RepoDir


    val qlist: MutableList[(String, String)] = MutableList()


    val users = getListOfUsers(repoDir)

    for (user <- users) {

      val queriesDir = RepoUtils().getScriptsDir(user)
      val queries = getListOfQueries(queriesDir)


      for ( query <-  queries) {
        //println("USER: "+user+" - QUERY: "+query)
        val text = getQueryText(queriesDir+"/"+query)
        qlist ++= List((user, text))
      }

    }

    // Take distinct queries
    val qdist = qlist.groupBy(_._2).map(e=> (e._1, e._2.head))
    //qdist.map(e=>e._2._1+"\n"+e._2._2).foreach(println)


    val xml = <queries>
      {qdist.map(e=>{<query user={e._2._1}>{e._2._2}</query>})}
    </queries>
    val formatter = new scala.xml.PrettyPrinter(80, 4)
    val formatted = formatter.format(xml)

    val outFull = outDir+"/"+"queries_"+java.lang.System.currentTimeMillis+".xml"

    saveXML(outFull, formatted)

  }

  private def getListOfUsers(dir: String):List[String] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isDirectory).map(_.getName).toList
    } else {
      List[String]()
    }
  }


  private def getListOfQueries(dir: String):List[String] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).map(_.getName).toList
    } else {
      List[String]()
    }
  }

  private def getQueryText(path: String): String = {
    Source.fromFile(path).getLines.mkString.trim
  }

  private def saveXML(filePath: String, text: String) = {
    val file = new File(filePath)
    val bw = new BufferedWriter(new FileWriter(file))
    println("Writing "+filePath)
    bw.write(text)
    bw.close()
  }
}
