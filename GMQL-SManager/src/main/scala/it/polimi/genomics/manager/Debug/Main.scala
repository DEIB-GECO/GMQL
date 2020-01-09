package it.polimi.genomics.manager.Debug

import java.io.File

import org.apache.log4j.{Level, Logger}
import org.slf4j
import org.slf4j.LoggerFactory

import scala.xml.XML

object Main {

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  val logger: slf4j.Logger = LoggerFactory.getLogger(Main.getClass)

  private final val usage: String = "TrainGen Options: \n" +
    "\t"+"-conf"+" : "+"folder containing the TrainGen configuration files;"

  private final val MAIN_FILE = "conf.xml"
  private final val GENERATOR_FILE = "generator.xml"
  private final val EXECUTOR_FILE = "executor.xml"
  private final val GMQL_CONF_DIR = "gmql_conf/"


  class Conf(confDir: String) {

    private val conf_xml = XML.load(confDir+"/"+MAIN_FILE)

    // Parse
    val properties: Map[String, String] = (conf_xml \\ "property").map(n=> (n \\ "name").text -> (n \\ "value").text).toMap[String, String]
    def tempDir : String = properties("temp_dir")
    def genDir: String = properties("gen_dir")
    def outDir: String = properties("out_dir")
    def skipGen: Boolean = properties("skip_gen") == "true"

    def log(): Unit = {
      println(s"TempDir: $tempDir \nOutDir: $outDir \nGenDir: $genDir \nSkipGen: $skipGen")
    }
  }


  def main(args: Array[String]): Unit = {

    var confDir =  "/Users/andreagulino/Desktop/conf/"

    // Read Options
    for (i <- args.indices if i % 2 == 0) {
      if ("-h".equals(args(i)) || "-help".equals(args(i))) {
        println(usage)
        System.exit(0)
      } else if ("-conf".equals(args(i))) {
        confDir = args(i + 1)
        logger.info("-conf: " + confDir)
      } else {
        logger.warn(s"Command option is not found ${args(i)}")
        System.exit(0)
      }
    }

    // Read main configuration file
    val conf = new Conf(confDir)

    conf.log()

    if(!conf.skipGen) {
      println("Generating Datasets: ")
      AutomatedGenerator.go(confDir+"/"+GENERATOR_FILE, conf.tempDir, conf.genDir)
    }


    // Query
    val dss = getListOfSubDirectories(conf.genDir)
    println(dss.mkString(","))








  }


  private def getListOfSubDirectories(directoryName: String): Array[String] = {
    (new File(directoryName))
      .listFiles
      .filter(_.isDirectory)
      .map(_.getAbsolutePath)
  }

}
