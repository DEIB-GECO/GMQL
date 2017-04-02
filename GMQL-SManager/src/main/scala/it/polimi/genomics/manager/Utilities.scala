package it.polimi.genomics.manager

import java.io.File

import org.slf4j.{Logger, LoggerFactory}

import scala.xml.{Elem, NodeSeq, XML}

/**
  * Created by abdulrahman on 08/02/2017.
  */
class Utilities {
  var SPARK_HOME: String = System.getenv("SPARK_HOME")
  var CLI_JAR: String = "GMQL-Cli-2.0-jar-with-dependencies.jar"
  var CLI_CLASS: String = "it.polimi.genomics.cli.GMQLExecuteCommand"
  var lib_dir_local: String = it.polimi.genomics.repository.Utilities().GMQLHOME + "/lib/"
  var lib_dir_hdfs: String = it.polimi.genomics.repository.Utilities().HDFSRepoDir + "/lib/"

  private val logger: Logger = LoggerFactory.getLogger(Utilities.getClass)

  //TODO: Acticvate the launcher modes
  var LAUNCHER_MODE: String =  LOCAL_LAUNCHER
  final val LOCAL_LAUNCHER: String = "LOCAL"
  final val CLUSTER_LAUNCHER: String = "CLUSTER"
  final val REMOTE_CLUSTER_LAUNCHER: String = "REMOTE_CLUSTER"

  var KNOX_GATEWAY: String = null
  var KNOX_SERVICE_PATH:String   = null
  var KNOX_USERNAME:String       = null
  var KNOX_PASSWORD:String       = null

  var LIVY_BASE_URL:String       = null
  var REMOTE_CLI_JAR_PATH:String = null

  def apply() = {

    try {
      val file: File = new File(it.polimi.genomics.repository.Utilities().getConfDir + "/impl.xml")
      val xmlFile: Elem = XML.loadFile(file)
      val properties: NodeSeq = (xmlFile \\ "property")
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
          case Conf.LAUNCHER_MODE => LAUNCHER_MODE = value

          case Conf.KNOX_GATEWAY => KNOX_GATEWAY = value
          case Conf.KNOX_SERVICE_PATH => KNOX_SERVICE_PATH = value
          case Conf.KNOX_USERNAME => KNOX_USERNAME = value
          case Conf.KNOX_PASSWORD => KNOX_PASSWORD = value
          case Conf.LIVY_BASE_URL => LIVY_BASE_URL = value
          case Conf.REMOTE_CLI_JAR_PATH => REMOTE_CLI_JAR_PATH = value

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
  val LAUNCHER_MODE = "LAUNCHER_MODE"
  val SPARK_HOME = "SPARK_HOME"
  val CLI_JAR_NAME = "CLI_JAR_NAME"
  val LIB_DIR_LOCAL = "LIB_DIR_LOCAL"
  val LIB_DIR_HDFS = "LIB_DIR_HDFS"
  val CLI_CLASS = "CLI_CLASS"

  val KNOX_GATEWAY = "KNOX_GATEWAY"
  val KNOX_SERVICE_PATH = "KNOX_SERVICE_PATH"
  val KNOX_USERNAME = "KNOX_USERNAME"
  val KNOX_PASSWORD = "KNOX_PASSWORD"
  val LIVY_BASE_URL = "LIVY_BASE_URL"
  val REMOTE_CLI_JAR_PATH = "REMOTE_CLI_JAR_PATH"
}