package it.polimi.genomics.manager

import java.io.File

import it.polimi.genomics.core.GDMSUserClass
import it.polimi.genomics.core.GDMSUserClass._
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
  var SPARK_UI_PORT:Int = 4040

  // User-category-specific spark property
  var USER_SPARK_PROP_NAME: String = "spark.cores.max"
  var USER_SPARK_PROP_VAL:  Map[GDMSUserClass, Long] = Map()


  private val logger: Logger = LoggerFactory.getLogger(Utilities.getClass)

  //TODO: Acticvate the launcher modes
  var LAUNCHER_MODE: String =  LOCAL_LAUNCHER
  final val LOCAL_LAUNCHER: String = "LOCAL"
  final val CLUSTER_LAUNCHER: String = "CLUSTER"
  final val REMOTE_CLUSTER_LAUNCHER: String = "REMOTE_CLUSTER"


  def apply() = {

    try {
      val file: File = new File(it.polimi.genomics.repository.Utilities().getConfDir + "/executor.xml")
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

          case Conf.SPARK_UI_PORT => SPARK_UI_PORT = value.toInt
          case Conf.SPARK_PROP_NAME => USER_SPARK_PROP_NAME = value.toString

          case Conf.GUEST_VALUE  => USER_SPARK_PROP_VAL += ( GDMSUserClass.GUEST  -> value.toLong)
          case Conf.BASIC_VALUE  => USER_SPARK_PROP_VAL += ( GDMSUserClass.BASIC  -> value.toLong)
          case Conf.PRO_VALUE    => USER_SPARK_PROP_VAL += ( GDMSUserClass.PRO    -> value.toLong)
          case Conf.ADMIN_VALUE  => USER_SPARK_PROP_VAL += ( GDMSUserClass.ADMIN  -> value.toLong)
          case Conf.PUBLIC_VALUE => USER_SPARK_PROP_VAL += ( GDMSUserClass.PUBLIC -> value.toLong)

          case _ => logger.error(s"Not known configuration property: $x, $value")
        }
        logger.debug(s"XML config override environment variables. $att = $value ")
      }
    } catch {
      case ex: Throwable => ex.printStackTrace(); logger.warn("XML config file is not found..")
    }

    if (SPARK_HOME == null) logger.warn("SPARK_HOME is not set .. To use Spark on Yarn platform, you should set Spark Home in the configuration file or as Environment varialble")

    if (USER_SPARK_PROP_VAL.size == 0) logger.warn("Custom Spark property not defined for any user category.")
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

  val SPARK_UI_PORT = "SPARK_UI_PORT"

  val SPARK_PROP_NAME = "SPARK_PROP_NAME"

  val GUEST_VALUE  = "GUEST_VALUE"
  val BASIC_VALUE  = "BASIC_VALUE"
  val PRO_VALUE    = "PRO_VALUE"
  val ADMIN_VALUE  = "ADMIN_VALUE"
  val PUBLIC_VALUE = "PUBLIC_VALUE"

}