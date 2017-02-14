package it.polimi.genomics.manager

import java.io.File

import org.slf4j.{Logger, LoggerFactory}

import scala.xml.XML

/**
  * Created by abdulrahman on 08/02/2017.
  */
class Utilities {
  var SPARK_HOME: String = System.getenv("SPARK_HOME")
  var CLI_JAR = "GMQL-Cli-2.0-jar-with-dependencies.jar"
  var CLI_CLASS = "it.polimi.genomics.cli.GMQLExecuteCommand"
  var lib_dir_local = it.polimi.genomics.repository.Utilities().GMQLHOME + "/lib/"
  var lib_dir_hdfs = it.polimi.genomics.repository.Utilities().HDFSRepoDir + "/lib/"
  private val logger: Logger = LoggerFactory.getLogger(Utilities.getClass)

  def apply() = {

    try {
      val file = new File(it.polimi.genomics.repository.Utilities().getConfDir + "/impl.conf")
      val xmlFile = XML.loadFile(file)
      val properties = (xmlFile \\ "property")
      //      val schemaType = (xmlFile \\ "gmqlSchema").head.attribute("type").get.head.text
      properties.map { x =>
        val att = x.attribute("name").get.head.text;
        val value = x.text;
        logger.debug(s"$att \t $value")
        att.toUpperCase() match {
          case Conf.SPARK_HOME => SPARK_HOME = value
          case Conf.CLI_JAR_NAME => CLI_JAR = value
          case Conf.LIB_DIR_HDFS => lib_dir_hdfs = value
          case Conf.LIB_DIR_LOCAL => lib_dir_local = value
          case Conf.CLI_CLASS => CLI_CLASS = value
          case _ => logger.error(s"Not known configuration property: $x, $value")
        }
        logger.debug(s"XML config override environment variables. $att = $value ")
      }
    } catch {
      case ex: Throwable => ex.printStackTrace(); logger.warn("XML config file is not found..")
    }

    if (SPARK_HOME == null) logger.warn("SPARK_HOME is not set .. To use Spark on Yarn platform, you should set Spark Home in the configuration file or as Environment varialble")
  }

  /**
    *  String of the location of the CLI jar, should be submitted for spark/flink
    * @return
    */
  def CLI_JAR_local(): String = lib_dir_local + CLI_JAR

  /**
    * String of the location of the CLI jar, should be submitted for spark/flink on HDFS
    * @return
    */
  def CLI_JAR_HDFS(): String = lib_dir_hdfs + CLI_JAR


}

object Utilities {
  private var instance: Utilities = null

  def apply(): Utilities = {
    if (instance == null) {
      instance = new Utilities();
      instance.apply()
    }
    instance
  }
}

/**
  * Set of configurations for Server Manager
  */
object Conf {
  val SPARK_HOME = "SPARK_HOME"
  val CLI_JAR_NAME = "CLI_JAR_NAME"
  val LIB_DIR_LOCAL = "LIB_DIR_LOCAL"
  val LIB_DIR_HDFS = "LIB_DIR_HDFS"
  val CLI_CLASS = "CLI_CLASS"
}