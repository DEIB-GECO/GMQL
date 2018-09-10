package it.polimi.genomics.repository

/**
  * Created by abdulrahman on 12/04/16.
  */

import java.io.File

import it.polimi.genomics.core.GDMSUserClass
import it.polimi.genomics.core.GDMSUserClass.GDMSUserClass
import it.polimi.genomics.repository.FSRepository.{DFSRepository, FS_Utilities, LFSRepository}
import it.polimi.genomics.repository.federated.GF_Decorator
import org.slf4j.{Logger, LoggerFactory}

import scala.xml.XML

/**
  *
  * @author abdulrahman kaitoua <abdulrahman dot kaitoua at polimi dot it>
  */
class Utilities() {

  import Utilities.logger
  var USERNAME: String = System.getProperty("user.name")
  //USERNAME  = System.getenv("USER")
  val HDFS: String = "HDFS"
  val LOCAL: String = "LOCAL"
  val REMOTE: String = "REMOTE"

  var RepoDir: String = "/tmp/repo/data/"
  var HDFSRepoDir: String = System.getenv("GMQL_DFS_HOME")
  // Mode can be only "HDFS" or "REMOTE"
  var MODE: String = System.getenv("GMQL_REPO_TYPE")
  var GMQL_REPO_TYPE: String = System.getenv("GMQL_REPO_TYPE")
  var CoreConfigurationFiles: String = null
  var HDFSConfigurationFiles: String = null
  var HADOOP_CONF_DIR:String = "/usr/local/Cellar/hadoop/2.7.2/libexec/etc/hadoop/"
  var GMQLHOME: String = System.getenv("GMQL_LOCAL_HOME")
  var HADOOP_HOME:String = System.getenv("HADOOP_HOME")
  var GMQL_CONF_DIR:String = null
  var REMOTE_HDFS_NAMESPACE:String = null

  // User Quota in KB
  var USER_QUOTA: Map[GDMSUserClass, Long] = Map()

  // GMQL Federated
  var GF_ENABLED = false
  var GF_NAMESERVER_ADDRESS: Option[String] = None
  var GF_NAMESPACE: Option[String] = None
  var GF_TOKEN: Option[String] =  None


  /**
    *  Read Configurations from the system environment variables.
    *  The xml configurations will override any environment variables configurations.
    */
  def apply(confDir:String = "../conf") = {
    try {
      var file = new File(confDir+"/repository.xml")
      val xmlFile =  if(file.exists()) XML.loadFile(file)
      else {
        logger.warn("Config file '"+confDir+"/repository.xml' does not exists, using default")
        XML.loadFile(new File("GMQL-Repository/src/main/resources/repository.xml"))
      }
      val properties = (xmlFile \\ "property")
      //      val schemaType = (xmlFile \\ "gmqlSchema").head.attribute("type").get.head.text

      properties.map { x =>
        val att = x.attribute("name").get.head.text;
        val value = x.text;
        logger.debug(s"$att \t $value")
        att.toUpperCase() match {
          case Conf.GMQL_DFS_HOME => this.HDFSRepoDir = value;
          case Conf.GMQL_LOCAL_HOME => this.GMQLHOME = value;
          case Conf.GMQL_REPO_TYPE =>
            this.MODE = value.toUpperCase() match {
              case HDFS => HDFS;
              case LOCAL => LOCAL
              case _ => HDFS
            };
            this.GMQL_REPO_TYPE = value
          case Conf.HADOOP_CONF_DIR  => HADOOP_CONF_DIR = value
          case Conf.HADOOP_HOME =>  HADOOP_HOME = value
          case Conf.GMQL_CONF_DIR => GMQL_CONF_DIR = value
          case Conf.REMOTE_HDFS_NAMESPACE => REMOTE_HDFS_NAMESPACE = value

          case Conf.DISK_QUOTA  => {

            val user_class =
              if( x.attribute("user-category").isDefined )
                GDMSUserClass.withNameOpt(x.attribute("user-category").get.head.text)
              else
                GDMSUserClass.ALL

            logger.info("Custom Disk Quota property for "+user_class+" set to "+value+" KB")

            USER_QUOTA += (user_class -> value.toLong)

          }


          case Conf.GF_ENABLED => GF_ENABLED =  (value == "true")
          case Conf.GF_NAMESERVER_ADDRESS => this.GF_NAMESERVER_ADDRESS = Some(value)
          case Conf.GF_NAMESPACE => this.GF_NAMESPACE = Some(value)
          case Conf.GF_TOKEN => this.GF_TOKEN = Some(value)

          case _ => logger.error(s"Not known configuration property: $x, $value")
        }
        logger.debug(s"XML config override environment variables. $att = $value ")
      }
    } catch {
      case ex: Throwable => ex.printStackTrace(); logger.warn("XML config file is not found..")
    }

    HADOOP_CONF_DIR =  if(HADOOP_CONF_DIR == null) HADOOP_HOME+"/etc/hadoop/" else HADOOP_CONF_DIR
    CoreConfigurationFiles =  HADOOP_CONF_DIR+"/core-site.xml"
    HDFSConfigurationFiles = HADOOP_CONF_DIR+"/hdfs-site.xml"

    this.GMQLHOME =  if (this.GMQLHOME == null)  "/tmp/repo/" else  this.GMQLHOME

    //let the GMQLHOME contains ~ as home folder of the user
    this.GMQLHOME = this.GMQLHOME .replaceFirst("^~", System.getProperty("user.home"))

    GMQL_CONF_DIR =  if (!(this.GMQL_CONF_DIR == null))  GMQL_CONF_DIR else confDir

    this.USERNAME  = if (this.USERNAME  == null) "gmql_user" else this.USERNAME

    this.HDFSRepoDir = if (this.HDFSRepoDir == null)   "/user/repo/" else this.HDFSRepoDir

    this.MODE = if (this.MODE == null) {
      logger.error("Environment variable GMQL_REPO_TYPE is empty... execution set to LOCAL")
      this.LOCAL
    } else this.MODE

    RepoDir = this.GMQLHOME + "/data/"

    //    CoreConfigurationFiles = if (CoreConfigurationFiles == null) "/usr/local/Cellar/hadoop/2.7.2/libexec/etc/hadoop/core-site.xml" else CoreConfigurationFiles
    //    HDFSConfigurationFiles = if (HDFSConfigurationFiles == null) "/usr/local/Cellar/hadoop/2.7.2/libexec/etc/hadoop/hdfs-site.xml" else HDFSConfigurationFiles
    logger.debug(CoreConfigurationFiles)
    logger.debug(HDFSConfigurationFiles)

    if (USER_QUOTA.size == 0) logger.warn("Disk quota is not defined for any user category.")
  }

  /**
    *
    * Constract the Directory to the tmp folder
    *
    * @param userName [[ String]] of the user name
    * @return Directory location of the temp folder
    */
  def getTempDir(userName: String = USERNAME): String = GMQLHOME + "/tmp/" + userName + "/"

  /**
    *
    * Constract the Directory to the regions folder on HDFS
    *
    * @param userName [[ String]] of the user name
    * @return Directory location of the regions folder in HDFS
    */
  def getHDFSRegionDir(userName: String = USERNAME): String = HDFSRepoDir + userName + "/regions/"


  /**
    * Retrieve the dag folder for each user on HDFS
    *
    * @param userName [[ String]] of the user name
    * @param create [[Boolean]] tells HDFS to create the folder or not
    * @return Directory location of the dag folder in HDFS
    *
    * */
  def getHDFSDagQueryDir(userName: String = USERNAME, create: Boolean = true): String = {
    val dag_dir = HDFSRepoDir + userName + "/dag/"

    if(create) {
      val creationMessage = if(FS_Utilities.createDFSDir(dag_dir)) "\t Dag folder created..." else "\t Dag folder not created..."
      logger.info( dag_dir + creationMessage)
    }
    dag_dir
  }

  /**
    * Retrieve the dag folder for each user on local
    *
    * @param userName [[ String]] of the user name
    * @param create [[Boolean]] tells local to create the folder or not
    * @return Directory location of the dag folder in local
    *
    * */
  def getDagQueryDir(userName: String = USERNAME, create: Boolean = true): String = {
    val dag_dir = RepoDir + userName + "/dag/"

    if(create) {
      val creationMessage = if(new File(dag_dir).mkdirs()) "\t Dag folder created..." else "\t Dag folder not created..."
      logger.info( dag_dir + creationMessage)
    }
    dag_dir
  }



  /**
    *
    * Construct the Directory to the user folder on HDFS
    *
    * @param userName [[ String]] of the user name
    * @return Directory location of the regions folder in HDFS
    */
  def getHDFSUserDir(userName: String = USERNAME): String = HDFSRepoDir + userName

  /**
    * Returns the dataset folder in HDFS
    * @param userName
    * @param DSName
    * @return
    */
  def getHDFSDSRegionDir(userName: String = USERNAME, DSName:String): String = HDFSRepoDir + userName + "/regions/" + DSName+"/"

  /**
    *
    * Constract the Directory to the regions folder on Local file system
    *
    * @param userName [[ String]] of the user name
    * @return Directory location of the regions folder in Local File system
    */
  def getRegionDir(userName: String = USERNAME): String = RepoDir + userName + "/regions/"


  /**
    *
    * Constructs the Directory to the regions folder on Local file system
    *
    * @param userName
    * @return
    */
  def getProfileDir(userName: String = USERNAME): String = RepoDir + userName + "/profiles/"


  /**
    *
    * Constructs the Directory to the regions folder on Local file system
    *
    * @param userName
    * @return
    */
  def getDSMetaDir(userName: String = USERNAME): String = RepoDir + userName + "/dsmeta/"



  /**
    *
    * Constract the Directory to the dataset folder
    *
    * @param userName [[ String]] of the user name
    * @return Directory location of the datasets folder
    */
  def getDataSetsDir(userName: String = USERNAME): String = RepoDir + userName + "/datasets/"

  /**
    *
    * Constract the Directory to the schema folder
    *
    * @param userName [[ String]] of the user name
    * @return Directory location of the schema folder
    */
  def getSchemaDir(userName: String = USERNAME): String = RepoDir + userName + "/schema/"

  /**
    *
    * Constract the Directory to the metadata folder
    *
    * @param userName [[ String]] of the user name
    * @return Directory location of the metadata folder
    */
  def getMetaDir(userName: String = USERNAME): String = RepoDir + userName + "/metadata/"

  /**
    *
    *  Constract the Directory to the indexes folder
    *
    * @param userName [[ String]] of the user name
    * @return Directory location of the indexes folder
    */
  def getIndexDir(userName: String = USERNAME): String = RepoDir + userName + "/indexes/"

  /**
    *
    * Constract the Directory to the GMQL scripts folder
    *
    * @param userName [[ String]] of the user name
    * @return Directory location of the GMQL scripts history folder
    */
  def getScriptsDir(userName: String = USERNAME): String = RepoDir + userName + "/queries/"

  /**
    *
    * Constract the Directory to the GMQL Repository folder for Specific user
    *
    * @param userName [[ String]] of the user name
    * @return Directory location of the repository user's folder
    */
  def getUserDir(userName: String = USERNAME): String = RepoDir + userName + "/"

  /**
    *
    * Get user quota in KB
    *
    * @param userClass [[GDMSUserClass]]
    * @return Quota in KB
    */
  def getUserQuota(userClass: GDMSUserClass): Long = {
    if( USER_QUOTA.isDefinedAt(GDMSUserClass.ALL) ) {
      USER_QUOTA(GDMSUserClass.ALL)
    } else if (USER_QUOTA.isDefinedAt(userClass) ) {
      USER_QUOTA(userClass)
    } else {
      logger.warn("Disk quota not defined for userClass "+userClass+" , assigning unlimited quota.")
      Long.MaxValue
    }
  }

  /**
    * Get the Directory to the Log folder
    * @param userName [[ String]] of the user name
    * @return Directory location of the logs folder
    */
  def getLogDir(userName: String = USERNAME): String = RepoDir + userName + "/logs/"

  /**
    * Get the directory of the log for the user
    * @param userName
    * @return
    */
  def getUserLogDir(userName: String = USERNAME): String = getLogDir(userName) + "user/"

  /**
    * Get the directory of the log for the developers
    * @param userName
    * @return
    */
  def getDevLogDir(userName: String = USERNAME): String = getLogDir(userName) + "dev/"

  /**
    *
    * Constract the Directory to the Configurations folder
    *
    * @return Directory location of the conf folder
    */
  def getConfDir() =  GMQL_CONF_DIR

  /**
    *
    * Set the home directory for GMQL.
    *
    * @param gmqlHome String of the path to the location of the home directory
    */
  def setGMQLHome(gmqlHome:String):Unit = {
    GMQLHOME = gmqlHome
  }

  /**
    * Set the configurations Directory for GMQL.
    *
    * @param confDir String of the location of the configuration directory
    */
  def setConfDir(confDir:String) = {
    GMQL_CONF_DIR = confDir
  }

  /**
    * Get the right instance of the repository according to the type specified
    * in the config file
    * @return an implementation of GMQLRepository
    */
  def getRepository(): GMQLRepository = {

    val repo =

    GMQL_REPO_TYPE match {
      case this.LOCAL  => new LFSRepository()
      case this.HDFS   => new DFSRepository()
    }

    if (GF_ENABLED) {
      println("GF_ENABLED")
      new GF_Decorator(repo)
    }
    else
      repo
  }

  def getHDFSNameSpace(): String = {
    if( GMQL_REPO_TYPE equals REMOTE )
     REMOTE_HDFS_NAMESPACE
    else
      FS_Utilities.gethdfsConfiguration().get("fs.defaultFS")
  }

}

object Utilities {
  private var instance: Utilities = null
  var confFolder: String = "../conf/"
  val logger: Logger = LoggerFactory.getLogger(Utilities.getClass)

  def apply(): Utilities = {
    if (instance == null) {
      instance = new Utilities();
      instance.apply(confFolder)
    }
    instance
  }
}

/**
  *  Configurations of GMQL
  */
object Conf {
  val GMQL_LOCAL_HOME = "GMQL_LOCAL_HOME"
  val GMQL_REPO_TYPE = "GMQL_REPO_TYPE"
  val GMQL_DFS_HOME = "GMQL_DFS_HOME"
  val HADOOP_HOME = "HADOOP_HOME"
  val HADOOP_CONF_DIR = "HADOOP_CONF_DIR"
  val GMQL_CONF_DIR = "GMQL_CONF_DIR"
  val REMOTE_HDFS_NAMESPACE = "REMOTE_HDFS_NAMESPACE"

  val GF_ENABLED = "GF_ENABLED"
  val GF_NAMESERVER_ADDRESS = "GF_NAMESERVER_ADDRESS"
  val GF_NAMESPACE = "GF_NAMESPACE"
  val GF_TOKEN = "GF_TOKEN"

  val DISK_QUOTA = "DISK_QUOTA"}