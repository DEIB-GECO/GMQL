package it.polimi.genomics.repository

import java.io.{File, FilenameFilter}

import it.polimi.genomics.core.DataStructures.IRDataSet
import it.polimi.genomics.core.ParsingType.PARSING_TYPE
import it.polimi.genomics.repository.FSRepository.{DFSRepository, LFSRepository}
import it.polimi.genomics.repository.{Utilities => u}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
/**
  * Created by abdulrahman on 18/01/2017.
  */
object RepositoryManager {
  private final val logger = LoggerFactory.getLogger(this.getClass)
  val usage =
    "\t.........................................................\n"+
    "\t.........................................................\n"+
    "\t...........GMQL Repository Manager help..................\n"+
    "\t.........................................................\n"+
    "\t.........................................................\n\n"+
    "\tRepositoryManager $COMMAND\n\n"+
    "\tAllowed Commands are :\n\n"+
    "\tRegisterUser\n\n"+
    "\t\tTo register the current user to the GMQL repository.\n"+
    "\tUnRegisterUser\n\n"+
    "\t\tTo unregister the current user from GMQL repository.\n"+
    "\t\tAll the datasets, history, User private schemata and user results will be deleted.\n"+
    "\t\tThe Samples on the Local File System will not be deleted.\n"+
    "\tCREATEDS DS_NAME SCHEMA_URL SAMPLES_PATH \n"+
    "\t\tDS_NAME is the new dataset name\n\n"+
    "\t\tSCHEMA_URL is the path to the schema file\n"+
    "\t\t - The schema can be a keyword { BED | BEDGRAPH | NARROWPEAK | BROADPEAK }\n"+
    "\t\tSAMPLES_PATH is the path to the samples, can be one of the following formats:\n"+
    "\t\t - Samples urls separated by a comma with no spaces in between:\n"+
    "\t\t   /to/the/path/sample1.bed,/to/the/path/sample2.bed \n"+
    "\t\t - Samples folder Path: /to/the/path/samplesFolder/ \n"+
    "\t\t   in this case all the samples with an associated metadata will be added to the dataset.\n"+
    "\t\tTIP: each sample file must have a metadata file with the same full name and aditional .meta extension \n"+
    "\t\t     for example: { sample.bed sample.bed.meta }\n\n"+
    "\tDELETEDS DS_NAME \n"+
    "\t\t To Delete a dataset named DS_NAME\n\n"+
    "\tADDSAMPLE DS_NAME SAMPLE_URL \n"+
    "\t\tDS_NAME the dataset name (It has to be already added in the system). \n"+
    "\t\tSAMPLE_URL is the path to the sample. No need to add the metadata Path since it must be in the same folder.\n"+
    "\t\t           For example: /to/the/path/sample.bed\n\n"+
    "\tDELETESAMPLE DS_NAME SAMPLE_URL \n"+
    "\t\tDelete one sample form the dataset named DS_NAME \n"+
    "\t\tSAMPLE_URL must be identical to what { LIST DS_NAME } command prints. \n\n"+
    "\tLIST ALL|DS_NAME\n"+
    "\t\tALL to print all the datasets for the current user and the public user. \n"+
    "\t\tDS_NAME to print the samples of this dataset\n\n"+
    "\tCopyDSToLocal DS_NAME LOCAL_DIRECTORY \n"+
    "\t\tThis command copy all the samples of DS_NAME to local folder.\n"+
    "\t\tThe samples will be copied with its metadata.\n"+
    "\t\tLOCAL_DIRECTORY is the full path to the local location. \n\n"+
    "INFO: For more information read the GMQL shell commands document.\n\n";

  var username = System.getProperty("user.name")
  val mr:GMQLRepository =
    if(Utilities().MODE.equals(Utilities().LOCAL))
      new LFSRepository()
    else
      new DFSRepository()


  def main(args: Array[String]): Unit = {

    if (args.length > 0 && ("h" == args(0) || "help" == args(0))) {
      logger.warn(usage)
      System.exit(0)
    }
    if (args.length == 0) {
      System.out.println("WARN:\tThe specified command is not supported.. \n" + "\tType { RepositoryManager h | help } for help...")
      return
    }

    val Command = args(0)
    Command.toLowerCase match {
      case "registeruser" =>
        if (args.length > 1) {
          username = args(1)
          Utilities().USERNAME = username
        }
        mr.registerUser(username)
      case "unregisteruser" =>
        if (args.length > 1) {
          username = args(1)
          Utilities().USERNAME = username
        }
        mr.unregisterUser(username)
      case "createds" =>
        if (args.length < 4) {
          logger.warn(usage)
          return
        }
        else if (args.length > 4) {
          username = args(4)
          Utilities().USERNAME = username
        }
        val confDir = new File(Utilities().getConfDir)
//        try{
          val schema = confDir.listFiles(new FilenameFilter() {
            def accept(dir: File, name: String): Boolean = name.toLowerCase.endsWith(".schema")
          })
          val URL = args(3)

          var samples = List[GMQLSample]()
          var i=0
          if (new File(URL).isDirectory) {
            val files = new File(URL).listFiles(new FilenameFilter() {
              def accept(dir: File, name: String): Boolean = {
                if (name.endsWith(".meta")) return false
                val f = new File(dir.toString + "/" + name + ".meta")
                logger.info(dir.toString + "/" + name + " => has meta file - " + f.exists)
                f.exists
              }
            })
            if (files.length == 0)
              logger.warn("The dataSet is empty.. \n\tCheck the files extensions. (i.e. sample.bed/sample.bed.meta)")
            for (file <- files)
              samples = samples :+ (
                new GMQLSample(file.getAbsolutePath,file.getAbsolutePath+".meta", {i += 1;i}.toString)
              )
          }
          else {
            val url = URL.split(",")
            for (u <- url) samples = samples :+ (new GMQLSample(u,u+".meta", {i += 1;i}.toString))
          }
          mr.importDs(args(1), username, samples.asJava, args(2))

//        }catch {
//          case ex: Throwable => logger.error("trace: "+ex.getMessage)
//        }
      case "deleteds" =>
        if (args.length < 2) {
          logger.warn(usage)
          return
        }
        else if (args.length > 2) {
          username = args(2)
          Utilities().USERNAME = username
        }
        System.out.println(args(1))
        if (args(1).endsWith("!")) {
          val datasetname = args(1).substring(0, args(1).length - 1)
          System.out.println("Delete all the datasets that starts with " + datasetname)
          val dataSetDir = new File(Utilities().getDataSetsDir(username))
          val datasets = dataSetDir.listFiles(new FilenameFilter() {def accept(dir: File, name: String): Boolean = name.matches(datasetname + "*.*\\.xml")})
          var i = 1
          while (i < datasets.length) {
              logger.info(s"datasets[$i] = " + datasets(i).getName.split("\\.")(0))
              i += 1;
          }
          val console = System.console
          val input = console.readLine("Are you sure you want to delete all (Y|N):")
          if (input.toLowerCase == "y") {
            var i = 0
            while (i < datasets.length) {
                mr.deleteDS(datasets(i).getName.split("\\.")(0), username)
                i += 1;
            }
          }
          else return
        }
        else mr.deleteDS(args(1), username)
      case "addsample" =>
        if (args.length < 3) {
          logger.warn(usage)
          return
        }
        else if (args.length > 3) {
          username = args(3)
          Utilities().USERNAME = username
        }
        mr.addSampleToDS(args(1), username, new GMQLSample( args(2), args(2)+".meta"))
      case "deletesample" =>
        if (args.length < 3) {
          logger.warn(usage)
          return
        }
        else if (args.length > 3) {
          username = args(3)
          Utilities().USERNAME = username
        }

        mr.deleteSampleFromDS(args(1), username, new GMQLSample( args(2), args(2)+".meta"))
      case "list" =>
        if (args.length < 2) {
          logger.warn(usage)
          return
        }
        else if (args.length > 2) {
          username = args(2)
          Utilities().USERNAME = username
        }
        if ("all" == args(1).toLowerCase) {
          val dss = mr.listAllDSs(username).asScala
          println(s"Total number of Datasets for user $username : ${dss.size}")
          dss.foreach(x=>println(x.position))
        }
        else {
          val samples = mr.listDSSamples(args(1), username).asScala
            println(s"Total number of samples in Dataset ${args(1)}: ${samples.size}")
            samples.foreach(x=>println(x.name))
        }
      case "copydstolocal" =>
        var DatasetName = ""
        var Locallocatoin = ""
        if (args.length < 3 || args.length > 4) {
          System.out.println(usage)
          return
        }
        DatasetName = args(1)
        Locallocatoin = args(2)
        if (args.length == 4) username = args(3)
        Utilities().USERNAME = username
        mr.exportDsToLocal(DatasetName,username, Locallocatoin)
      case _ =>
        logger.error("The Command is not defined....")
        logger.warn(usage)
    }
  }
}
