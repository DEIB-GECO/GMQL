package it.polimi.genomics.manager

import java.io.File

import it.polimi.genomics.core.GDMSUserClass
import org.slf4j.{Logger, LoggerFactory}

import scala.xml.{Elem, NodeSeq, XML}
import scala.collection.mutable.Map

/**
  * Created by abdulrahman on 08/02/2017.
  */
class Utilities {

  import Utilities.logger


  var SPARK_HOME: String = System.getenv("SPARK_HOME")
  var CLI_JAR: String = "GMQL-Cli-2.0-jar-with-dependencies.jar"
  var CLI_CLASS: String = "it.polimi.genomics.cli.GMQLExecuteCommand"
  var lib_dir_local: String = it.polimi.genomics.repository.Utilities().GMQLHOME + "/lib/"
  var lib_dir_hdfs: String = it.polimi.genomics.repository.Utilities().HDFSRepoDir + "/lib/"
  var SPARK_UI_PORT:Int = 4040

  // User-category-specific spark property
  var SPARK_CUSTOM: Map[String, Map[GDMSUserClass.Value, String]] = Map()

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
          case Conf.SPARK_CUSTOM  => {

            val user_class = GDMSUserClass.withNameOpt(x.attribute("user-category").get.head.text)
            val spark_property = x.attribute("spark-property").get.head.text

            logger.info("Custom Spark property for "+user_class+": "+spark_property+"="+value)

            if ( SPARK_CUSTOM.isDefinedAt(spark_property) ) {
              SPARK_CUSTOM(spark_property) += (user_class -> value)
            } else {
              SPARK_CUSTOM += (spark_property -> Map(user_class -> value))
            }
          }
          case _ => logger.error(s"Not known configuration property: $x, $value")
        }
        logger.debug(s"XML config override environment variables. $att = $value ")
      }
    } catch {
      case ex: Throwable => ex.printStackTrace(); logger.warn("XML config file is not found..")
    }

    if (SPARK_HOME == null) logger.warn("SPARK_HOME is not set .. To use Spark on Yarn platform, you should set Spark Home in the configuration file or as Environment varialble")

    if (SPARK_CUSTOM.size == 0) logger.warn("Custom Spark property not defined for any user category.")
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

  val logger: Logger = LoggerFactory.getLogger(Utilities.getClass)

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
  val SPARK_CUSTOM = "SPARK_CUSTOM"
}