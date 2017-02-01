package it.polimi.genomics.repository

/**
  * Created by abdulrahman on 12/04/16.
  */

import java.io.File

import org.slf4j.{Logger, LoggerFactory}

import scala.xml.XML

/**
  *
  * @author abdulrahman kaitoua <abdulrahman dot kaitoua at polimi dot it>
  */
class Utilities() {
  private val logger: Logger = LoggerFactory.getLogger(Utilities.getClass)
  var USERNAME: String = System.getProperty("user.name")
  val HDFS: String = "YARN"
  val LOCAL: String = "LOCAL"

  var RepoDir: String = null
  var HDFSRepoDir: String = null
  var MODE: String = null
  var CoreConfigurationFiles: String = null
  var HDFSConfigurationFiles: String = null
  var GMQLHOME: String = null


  /**
    *  Read Configurations from the system environment variables.
    *  The xml configurations will override any environment variables configurations.
    */
  def apply() = {
    var gmql: String = System.getenv("GMQL_HOME")
    val user: String = System.getenv("USER")
    var dfs: String = System.getenv("GMQL_DFS_HOME")
    var exec: String = System.getenv("GMQL_EXEC")
    var coreConf: String = System.getenv("HADOOP_CONF_DIR")
    var hdfsConf: String = System.getenv("HADOOP_CONF_DIR")

    try {
      val file = new File("../conf/GMQL.conf")
      val xmlFile = XML.loadFile(file)
      val properties = (xmlFile \\ "property")
      //      val schemaType = (xmlFile \\ "gmqlSchema").head.attribute("type").get.head.text

      properties.map { x =>
        val att = x.attribute("name").get.head.text;
        val value = x.text;
        att.toUpperCase() match {
          case Conf.GMQL_DFS_HOME => dfs = value;
          case Conf.GMQL_HOME => gmql = value;
          case Conf.GMQL_EXEC => exec = value.toUpperCase();
          case Conf.HADOOP_HOME => {
            coreConf = value + "/etc/hadoop/";
            hdfsConf = value + "/etc/hadoop/";
          }
          case _ => logger.error(s"Not known configuration property: $x, $value")
        }
        logger.debug(s"XML config override environment variables. $att = $value ")
      }
    } catch {
      case ex: Throwable => ex.printStackTrace(); logger.warn("XML config file is not found..")
    }


    if (gmql == null) this.GMQLHOME = "/Users/abdulrahman/gmql_repository"
    else this.GMQLHOME = gmql

    if (user == null) this.USERNAME = "gmql_user"
    else this.USERNAME = user

    if (dfs == null) this.HDFSRepoDir = "/user/akaitoua/gmql_repo/"
    else this.HDFSRepoDir = dfs

    if (exec == null) {
      logger.error("Environment variable GMQL_EXEC is empty... execution set to LOCAL")
      this.MODE = this.HDFS
    } else this.MODE = exec.toUpperCase

    RepoDir = this.GMQLHOME + "/data/"


    CoreConfigurationFiles = (if (coreConf == null) "/usr/local/Cellar/hadoop/2.7.2/libexec/etc/hadoop/" else coreConf) + "/core-site.xml"
    HDFSConfigurationFiles = (if (hdfsConf == null) "/usr/local/Cellar/hadoop/2.7.2/libexec/etc/hadoop/" else hdfsConf) + "/hdfs-site.xml"
    logger.debug("GMQL_HOME is set to = " + gmql + "," + this.GMQLHOME)
    logger.debug("GMQL_DFS_HOME, HDFS Repository is set to = " + dfs + "," + this.HDFSRepoDir)
    logger.debug("MODE is set to = " + exec + "," + this.MODE)
    logger.debug("User is set to = " + user + "," + this.USERNAME)
  }

  def deleteFromLocalFSRecursive(dir: File) {
    var files: Array[File] = null
    if (dir.isDirectory) {
      files = dir.listFiles
      if (!(files == null)) {
        for (file <- files) {
          deleteFromLocalFSRecursive(file)
        }
      }
      dir.delete
    }
    else dir.delete
  }

  /**
    *
    * @param userName
    * @return
    */
  def getTempDir(userName: String = USERNAME): String = GMQLHOME + "/tmp/" + userName + "/"

  /**
    *
    * @param userName
    * @return
    */
  def getHDFSRegionDir(userName: String = USERNAME): String = HDFSRepoDir + userName + "/regions/"

  /**
    *
    * @param userName
    * @return
    */
  def getLocalRegionDir(userName: String = USERNAME): String = RepoDir + userName + "/regions/"

  /**
    *
    * @param userName
    * @return
    */
  def getRegionDir(userName: String = USERNAME): String = RepoDir + userName + "/regions/"

  /**
    *
    * @param userName
    * @return
    */
  def getDataSetsDir(userName: String = USERNAME): String = RepoDir + userName + "/datasets/"

  /**
    *
    * @param userName
    * @return
    */
  def getSchemaDir(userName: String = USERNAME): String = RepoDir + userName + "/schema/"

  /**
    *
    * @param userName
    * @return
    */
  def getMetaDir(userName: String = USERNAME): String = RepoDir + userName + "/metadata/"

  /**
    *
    * @param userName
    * @return
    */
  def getIndexDir(userName: String = USERNAME): String = RepoDir + userName + "/indexes/"

  /**
    *
    * @param userName
    * @return
    */
  def getScriptsDir(userName: String = USERNAME): String = RepoDir + userName + "/queries/"

  /**
    *
    * @param userName
    * @return
    */
  def getUserDir(userName: String = USERNAME): String = RepoDir + userName + "/"

  /**
    *
    * @param userName
    * @return
    */
  def getLogDir(userName: String = USERNAME) = GMQLHOME + "/data/" + userName + "/logs/"

  /**
    *
    * @return
    */
  def getConfDir = GMQLHOME + "/conf/"
}

object Utilities {
  private var instance: Utilities = null

  def apply(): Utilities = {
    if (instance == null) {
      instance = new Utilities(); instance.apply()
    }
    instance
  }
}

object Conf {
  val GMQL_HOME = "GMQL_HOME"
  val GMQL_EXEC = "GMQL_EXEC"
  val GMQL_DFS_HOME = "GMQL_DFS_HOME";
  val HADOOP_HOME = "HADOOP_HOME";
}


