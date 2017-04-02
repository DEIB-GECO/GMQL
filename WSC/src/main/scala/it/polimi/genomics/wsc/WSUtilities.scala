package it.polimi.genomics.wsc

import java.io.File

import org.slf4j.{Logger, LoggerFactory}

import scala.xml.{Elem, NodeSeq, XML}

/**
  * Created by andreagulino on 02/04/17.
  */
class WSUtilities {

  private val logger: Logger = LoggerFactory.getLogger(WSUtilities.getClass)

  //todo: find better solution
  val CONF_DIR = "../conf"

  var KNOX_GATEWAY: String       = _
  var KNOX_SERVICE_PATH:String   = _
  var KNOX_USERNAME:String       = _
  var KNOX_PASSWORD:String       = _

  var LIVY_BASE_URL:String       = _
  var CLI_JAR_PATH:String        = _
  var CLI_CLASS:String           = _

  def apply() = {

    try {
      val file: File = new File(CONF_DIR+ "/remote.xml")
      val xmlFile: Elem = XML.loadFile(file)
      val properties: NodeSeq = (xmlFile \\ "property")
      //      val schemaType = (xmlFile \\ "gmqlSchema").head.attribute("type").get.head.text
      properties.map { x =>
        val att = x.attribute("name").get.head.text;
        val value = x.text;
        logger.debug(s"$att \t $value")
        att.toUpperCase() match {
          case Conf.KNOX_GATEWAY => KNOX_GATEWAY = value
          case Conf.KNOX_SERVICE_PATH => KNOX_SERVICE_PATH = value
          case Conf.KNOX_USERNAME => KNOX_USERNAME = value
          case Conf.KNOX_PASSWORD => KNOX_PASSWORD = value
          case Conf.LIVY_BASE_URL => LIVY_BASE_URL = value
          case Conf.CLI_JAR_PATH  => CLI_JAR_PATH  = value
          case Conf.CLI_CLASS     => CLI_CLASS     = value

          case _ => logger.error(s"Not known configuration property: $x, $value")
        }
        logger.debug(s"XML config override environment variables. $att = $value ")
      }
    } catch {
      case ex: Throwable => ex.printStackTrace(); logger.warn("XML config file is not found..")
    }
  }

}

object WSUtilities {
  private var instance: WSUtilities = null

  def apply(): WSUtilities = {
    if (instance == null) {
      instance = new WSUtilities();
      instance.apply()
    }
    instance
  }
}


/**
  * Set of configurations for Server Manager
  */
object Conf {
  val CLI_JAR_PATH = "CLI_JAR_PATH"
  val CLI_CLASS = "CLI_CLASS"
  val KNOX_GATEWAY = "KNOX_GATEWAY"
  val KNOX_SERVICE_PATH = "KNOX_SERVICE_PATH"
  val KNOX_USERNAME = "KNOX_USERNAME"
  val KNOX_PASSWORD = "KNOX_PASSWORD"
  val LIVY_BASE_URL = "LIVY_BASE_URL"
}
