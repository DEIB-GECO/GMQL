package it.polimi.genomics.manager

import java.io.{File, IOException}
import java.nio.file.{Files, Paths}
import java.text.SimpleDateFormat
import java.util.Date

import it.polimi.genomics.core._
import it.polimi.genomics.manager.Launchers.GMQLLocalLauncher
import it.polimi.genomics.repository.FSRepository.{DFSRepository, LFSRepository, XMLDataSetRepository}
import it.polimi.genomics.repository.{Utilities => repo_Utilities}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
  * Created by abdulrahman on 31/01/2017.
  */
object CLI {
  private final val logger = LoggerFactory.getLogger(this.getClass) //GMQLExecuteCommand.getClass);

  val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
  System.setProperty("current.date", dateFormat.format(new Date()));

  private final val SYSTEM_TMPE_DIR: String = System.getProperty("java.io.tmpdir")
  private final val DEFAULT_SCHEMA_FILE:String = "/test.schema";
  private final val date: String = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());

  private final val usage: String = "GMQL-Submit " +
    " [-username USER] " +
    "[-exec FLINK|SPARK] [-binsize BIN_SIZE] [-jobid JOB_ID] " +
    "[-verbose true|false] " +
    "[-outputFormat GTF|TAB]" +
    "[-outputCoordinateSystem 0-based|1-based|default]" +
    "-scriptpath /where/gmql/script/is \n" +
    "\n" +
    "\n" +
    "Description:\n" +
    "\t[-username USER]\n" +
    "\t\tThe default user is the user the application is running on $USER.\n" +
    "\n" +
    "\t[-exec FLINK|SPARK] \n" +
    "\t\tThe execution type, Currently Spark and Flink engines are supported as platforms for executing GMQL Script.\n" +
    "\n" +
    "\t[-verbose true|false]\n" +
    "\t\tThe default will print only the INFO tags. -verbose is used to show Debug mode.\n" +
    "\n" +
    "\t[-outputFormat GTF|TAB]\n" +
    "\t\tThe default output format is TAB: tab delimited files in the format of BED files." +
    "\n" +
    "\t[-outputCoordinateSystem 0-based|1-based|default]\n" +
    "\t\tThe default output coordinate system for GTF output format is 1-based, for TAB output format is 0-based." +
    "\n" +
    "\t-scriptpath /where/gmql/script/is/located\n" +
    "\t\tManditory parameter, select the GMQL script to execute"

  def main(args: Array[String]): Unit = {

    try{
      //    DOMConfigurator.configure("GMQL-Core/logback.xml")
      val root:ch.qos.logback.classic.Logger = org.slf4j.LoggerFactory.getLogger("org.apache.spark").asInstanceOf[ch.qos.logback.classic.Logger];
      root.setLevel(ch.qos.logback.classic.Level.WARN);
//      org.slf4j.LoggerFactory.getLogger("it.polimi.genomics.manager").asInstanceOf[ch.qos.logback.classic.Logger].setLevel(ch.qos.logback.classic.Level.INFO)
    }catch{
      case _:Throwable => logger.warn("log4j.xml is not found in conf")
    }

    //Setting the default options
    var executionType: String = ImplementationPlatform.SPARK.toString.toLowerCase();
    var scriptPath: String = null;
    var username: String = System.getProperty("user.name")
    var outputPath: String = ""
    var outputFormat: GMQLSchemaFormat.Value = GMQLSchemaFormat.TAB
    var outputCoordinateSystem: GMQLSchemaCoordinateSystem.Value = GMQLSchemaCoordinateSystem.Default
    var verbose: Boolean = false
    var i = 0;

    for (i <- 0 until args.length if (i % 2 == 0)) {
      if ("-h".equals(args(i)) || "-help".equals(args(i))) {
        println(usage)

      } else if ("-exec".equals(args(i))) {
        executionType = args(i + 1).toLowerCase()
        logger.info("Execution Type is set to: " + executionType)

      }  else if ("-username".equals(args(i))) {
        username = args(i + 1).toLowerCase()
        logger.info("Username set to: " + username)

      }  else if ("-verbose".equals(args(i).toLowerCase())) {
        if(args(i+1).equals("true"))verbose = true else verbose = false
        logger.info("Output is set to verbose: " + verbose)

      } else if ("-scriptpath".equals(args(i))) {
        val sFile = new File (args(i + 1))
        if(!sFile.exists()) {
          logger.error(s"Script file not found $scriptPath")
          return 0
        };
        scriptPath = sFile.getPath
        logger.info("scriptpath set to: " + scriptPath)

      } else if ("-outputformat".equals(args(i).toLowerCase())) {
        val out = args(i + 1).toUpperCase().trim
        outputFormat =
          if(out == GMQLSchemaFormat.TAB.toString)
            GMQLSchemaFormat.TAB
          else if(out == GMQLSchemaFormat.GTF.toString)
            GMQLSchemaFormat.GTF
          else {
            logger.warn(s"Not known format $out, Setting the output format for ${GMQLSchemaFormat.TAB}")
            GMQLSchemaFormat.TAB
          }
        logger.info(s"Output Format set to: $out" + outputFormat)

      } else if ("-outputCoordinateSystem".equals(args(i).toLowerCase())) {
        val out = args(i + 1).toUpperCase().trim
        outputCoordinateSystem =
          if(out == GMQLSchemaCoordinateSystem.ZeroBased.toString)
            GMQLSchemaCoordinateSystem.ZeroBased
          else if(out == GMQLSchemaCoordinateSystem.OneBased.toString)
            GMQLSchemaCoordinateSystem.OneBased
          else if (out == GMQLSchemaCoordinateSystem.Default.toString)
            GMQLSchemaCoordinateSystem.Default
          else {
            logger.warn(s"Not known coordinate system $out, Setting the output coordinate system for ${GMQLSchemaCoordinateSystem.Default}")
            GMQLSchemaCoordinateSystem.Default
          }
        logger.info(s"Output Coordinate system set to: $out" + outputCoordinateSystem)

      }
      else
        {
          logger.warn(s"Command option is not found ${args(i)}")
        }
    }

    val gmqlScript: GMQLScript = try{
    //GMQL script
     new GMQLScript( new String(Files.readAllBytes(Paths.get(scriptPath))),scriptPath)
    }catch
      {
        case x:IOException => x.printStackTrace();new GMQLScript("","")
      }
    //Default bin parameters
    val binSize: BinSize = new BinSize(5000, 5000, 1000)

    // Set the repository based on the global variables.
    val repository: XMLDataSetRepository = if(repo_Utilities().MODE == repo_Utilities().HDFS) new DFSRepository() else new LFSRepository()

    //Spark context setting
    // number of executers is set to the number of the running machine cores.
    val conf: SparkConf = new SparkConf()
      .setAppName("GMQL V2 Spark")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.kryoserializer.buffer", "128")
      .set("spark.driver.allowMultipleContexts","true")
      .set("spark.sql.tungsten.enabled", "true")
    val sc:SparkContext =new SparkContext(conf)

    //GMQL context contains all the GMQL job needed information
    val gmqlContext: GMQLContext = new GMQLContext(ImplementationPlatform.SPARK, repository, outputFormat, outputCoordinateSystem, binSize, username,sc)

    //create GMQL server manager instance, if it is not created yet.
    val server: GMQLExecute = GMQLExecute()

    //register Job
    val job = server.registerJob(gmqlScript, gmqlContext, "")

    //Run the job
    server.execute(job.jobId, new GMQLLocalLauncher(job))
  }
}
