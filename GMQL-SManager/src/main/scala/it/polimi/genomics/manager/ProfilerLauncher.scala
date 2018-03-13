package it.polimi.genomics.manager
import java.io.File

import it.polimi.genomics.manager.CLI.logger

import scala.collection.JavaConversions._
import it.polimi.genomics.profiling.Profilers.Profiler
import it.polimi.genomics.repository.FSRepository.{DFSRepository, FS_Utilities}
import it.polimi.genomics.repository.FSRepository.datasets.GMQLDataSetXML
import it.polimi.genomics.repository.GMQLExceptions.GMQLDSNotFound
import org.slf4j.LoggerFactory
import it.polimi.genomics.repository.{GMQLRepository, Utilities => RepoUtilities}
import it.polimi.genomics.spark.utilities.ProfilerLoader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.xml.DOMConfigurator

/**
  * Created by andreagulino on 30/09/17.
  */
object ProfilerLauncher {

  private final val logger = LoggerFactory.getLogger(this.getClass)
  private var repo: GMQLRepository = null

  private final val usage: String = "ProfileLauncher " +
    " -username USER " +
    " -dataset [NAME|ALL] "+
    " [-configFolder PATH]"


  def main(args: Array[String]): Unit = {

    try{
      //    DOMConfigurator.configure("GMQL-Core/logback.xml")
      val root:ch.qos.logback.classic.Logger = org.slf4j.LoggerFactory.getLogger("org.apache.spark").asInstanceOf[ch.qos.logback.classic.Logger];
      root.setLevel(ch.qos.logback.classic.Level.WARN);
      //      org.slf4j.LoggerFactory.getLogger("it.polimi.genomics.manager").asInstanceOf[ch.qos.logback.classic.Logger].setLevel(ch.qos.logback.classic.Level.INFO)
    }catch{
      case _:Throwable => logger.warn("log4j.xml is not found in conf")
    }

    var username: Option[String] = Some("public")
    var dataset: Option[String] = None
    var configFolder = "./config/"

    for (i <- 0 until args.length if (i % 2 == 0)) {
      if ("-h".equals(args(i)) || "-help".equals(args(i))) {
        println(usage)
        System.exit(0)

      } else if ("-username".equals(args(i))) {
        username = Some(args(i + 1))
        logger.info("Username: " + username.get)

      } else if ("-dataset".equals(args(i))) {
        dataset = Some(args(i + 1))
        logger.info("Dataset set to: " + dataset.get)

      } else if ("-configFolder".equals(args(i))) {
        configFolder = args(i + 1)
        logger.info("ConfigFolder set to: " + configFolder)

      } else {
        logger.warn(s"Command option is not found ${args(i)}")
        System.exit(0)
      }
    }


    if (username.isEmpty || dataset.isEmpty) {
      logger.error("Username or Dataset not provided.")
      logger.info(usage)
      System.exit(-1)
    }



    RepoUtilities.confFolder = configFolder
    repo = new DFSRepository()

    var datasets: List[String] = List[String]()

    if (dataset.get.toUpperCase.equals("ALL")) {
      datasets = repo.listAllDSs(username.get).map(_.position).toList
    } else {
      try {
        logger.info("Calling listDSSamples with username '"+username.get+"' and dsname '"+dataset.get+"'")
         repo.listDSSamples(dataSetName = dataset.get, userName =  username.get)
      } catch {
        case _: GMQLDSNotFound => {
          logger.error("Dataset not found."); System.exit(-1)
        }
      }
      datasets = List(dataset.get)
    }


    datasets.foreach(x => {profileDS(username.get, x)})

  }


  def profileDS(username: String, dsname: String): Unit = {

    if( repo == null ) {
      repo = RepoUtilities().getRepository()
    }

    val conf = FS_Utilities.gethdfsConfiguration

    val profilesFolder  = RepoUtilities().getProfileDir(username)

//    val dssamplepath = (new GMQLDataSetXML(dsname, username).loadDS()).dataSet.position

    val sample_path   = repo.listDSSamples(dsname, username)(0).name
    val sample_folder = if(sample_path.lastIndexOf("/") >= 0) sample_path.substring(0, sample_path.lastIndexOf("/")+1) else sample_path


    logger.info("\n\n Sample path: "+sample_path+"\n\n")
    var dspath       = RepoUtilities().getHDFSRegionDir(username)+"/"+sample_folder

    logger.info("\n\n Profiling path: "+dspath+"\n\n")

    // dssamplepath.substring(0, dssamplepath.lastIndexOf("/")+1)


    val profile = ProfilerLoader.profile(dspath, conf)

    try {

      val path = new org.apache.hadoop.fs.Path(dspath);
      val fs = FileSystem.get(path.toUri(), conf);
      val output = fs.create(new Path(dspath +  "/profile.xml"));
      val output_web = fs.create(new Path(dspath + "/web_profile.xml"));

      val os = new java.io.BufferedOutputStream(output)
      val os_web = new java.io.BufferedOutputStream(output_web)

      os.write(Profiler.profileToOptXML(profile).toString().getBytes("UTF-8"))
      os_web.write(Profiler.profileToWebXML(profile).toString().getBytes("UTF-8"))

      os.close()
      os_web.close()
    } catch {
      case e: Throwable => {
        logger.error(e.getMessage)
        e.printStackTrace()
      }
    }

    // Copy from remote to local
    FS_Utilities.copyfiletoLocal(dspath+"/web_profile.xml", profilesFolder+"/"+dsname+".profile")

  }



}
