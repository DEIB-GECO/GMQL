package it.polimi.genomics.cli

import java.io._
import java.text.SimpleDateFormat
import java.util.Date

import it.polimi.genomics.GMQLServer.{GmqlServer, Implementation}
import it.polimi.genomics.compiler._
import it.polimi.genomics.core.{DAGSerializer, DAGWrapper}
//import it.polimi.genomics.core.DataStructures.IRVariable
import it.polimi.genomics.core.{GMQLSchemaCoordinateSystem, GMQLSchemaFormat, ImplementationPlatform}
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.xml.DOMConfigurator
import org.apache.log4j.{FileAppender, Level, PatternLayout, ConsoleAppender}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j._

/**
  * Created by Abdulrahman Kaitoua on 10/09/15.
  * Email: abdulrahman.kaitoua@polimi.it
  *
  */

object GMQLExecuteCommand {
  private final val logger = LoggerFactory.getLogger(/*Logger.ROOT_LOGGER_NAME)*/ GMQLExecuteCommand.getClass);
  org.apache.log4j.Logger.getRootLogger().getLoggerRepository().resetConfiguration()

  val console = new ConsoleAppender() //create appender
  //configure the appender
  val PATTERN = "%d [%p|%c|%C{1}] %m%n"
  console.setLayout(new PatternLayout(PATTERN))
  console.setThreshold(Level.ALL)
  console.activateOptions()
  //add appender to any Logger (here is root)
  org.apache.log4j.Logger.getRootLogger().addAppender(console)
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd");
  System.setProperty("current.date", dateFormat.format(new Date()));

  private final val SYSTEM_TMPE_DIR = System.getProperty("java.io.tmpdir")
  private final val DEFAULT_SCHEMA_FILE:String = "/test.schema";
  private final val date = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());

  private final val usage = "GMQL-Submit " +
    " [-username USER] " +
    "[-exec FLINK|SPARK] [-binsize BIN_SIZE] [-jobid JOB_ID] " +
//    "[-script G = SELECT();MATERIALIZE G;] " +
//    "[-inputs DS1:/location/on/HDFS/,DS2:/location/on/HDFS] " +
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
    "\t[-binsize BIN_SIZE]\n" +
    "\t\tBIN_SIZE is a Long value set for Genometric Map and Genometric Join operations. \n" +
    "\t\tDense data needs smaller bin size. Default is 5000.\n"+
    "\n"+
    "\t[-jobid JOBID]\n" +
    "\t\tThe default JobID is the username concatenated with a time stamp and the script file name.\n" +
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

  /**
    * CLI: Command Line Interface to control GMQL-Submit Command.
    * @param args
    */
  def main(args: Array[String]) {
    logger.info(args.mkString("\t"))
    logger.info("Try to execute GMQL Script..")

    //Setting the default options
    var executionType = ImplementationPlatform.SPARK.toString.toLowerCase();
    var bin = 5000l;
    var scriptPath: String = null;
    var script: String = null
    var username: String = System.getProperty("user.name")
    var outputPath = ""
    var jobid = ""
    var outputFormat = GMQLSchemaFormat.TAB
    var outputCoordinateSystem = GMQLSchemaCoordinateSystem.Default
    var schemata = Map[String, String]()
    var inputs = Map[String, String]()
    var outputs = Map[String, String]()
    var logDir:String = null
    var verbose = false
    var i = 0;

    // DAG OPTIONS
    var dag : Option[DAGWrapper] = None
    var dagPath : String = null

    //Check the CLI options
    for (i <- 0 until args.length if (i % 2 == 0)) {
      if ("-h".equals(args(i)) || "-help".equals(args(i))) {
        println(usage)

      } else if ("-exec".equals(args(i))) {
        executionType = args(i + 1).toLowerCase()
        logger.info("Execution Type is set to: " + executionType)

      } else if ("-bin".equals(args(i))) {
        bin = args(i + 1).toLong
        logger.info("Bin size set to: " + bin)

      } else if ("-username".equals(args(i))) {
        username = args(i + 1).toLowerCase()
        logger.info("Username set to: " + username)

      } else if ("-jobid".equals(args(i))) {
        jobid = args(i + 1).toLowerCase()
        logger.info("JobID set to: " + jobid)

      } else if ("-verbose".equals(args(i).toLowerCase())) {
        if(args(i+1).equals("true"))verbose = true else verbose = false
        logger.info("Output is set to Verbose: " + verbose)

      } else if ("-script".equals(args(i).toLowerCase())) {
        script = args(i + 1)
        logger.info("Output Format set to: " + outputFormat)

      } else if ("-scriptpath".equals(args(i))) {
        val sFile = new File (args(i + 1))
        if(!sFile.exists()) {
          logger.error(s"Script file not found $scriptPath")
//          return 0
        };
        scriptPath = sFile.getPath
        logger.info("scriptpath set to: " + scriptPath)
      } else if ("-logDir".equals(args(i))) {
        logDir = args(i + 1)
        logger.info("Log Directory is set to: " + logDir)

      } else if ("-outputformat".equals(args(i).toLowerCase())) {
        val out = args(i + 1).toLowerCase().trim
        outputFormat =
          if(out == GMQLSchemaFormat.TAB.toString)
            GMQLSchemaFormat.TAB
          else if(out == GMQLSchemaFormat.GTF.toString)
            GMQLSchemaFormat.GTF
          else {
            logger.warn(s"Not knwon format $out, Setting the output format for ${GMQLSchemaFormat.TAB}")
            GMQLSchemaFormat.TAB
          }
        logger.info(s"Output Format set to: $out" + outputFormat)

      } else if ("-outputcoordinatesystem".equals(args(i).toLowerCase())) {
        val out = args(i + 1).toLowerCase().trim
        outputCoordinateSystem =
          if (out == GMQLSchemaCoordinateSystem.ZeroBased.toString.toLowerCase)
            GMQLSchemaCoordinateSystem.ZeroBased
          else if (out == GMQLSchemaCoordinateSystem.OneBased.toString.toLowerCase)
            GMQLSchemaCoordinateSystem.OneBased
          else if (out == GMQLSchemaCoordinateSystem.Default.toString.toLowerCase)
            GMQLSchemaCoordinateSystem.Default
          else {
            logger.warn(s"Not knwon coordinate system $out, Setting the output coordinate system for ${GMQLSchemaCoordinateSystem.Default}")
            GMQLSchemaCoordinateSystem.Default
          }
        logger.info(s"Output Coordinate System set to: $out" + outputCoordinateSystem)
      }
      else if ("-inputDirs".equals(args(i))) {
        //List of [NAME:::Dir] separated by comma
        //Input Datasets directories.
        // We added this option to GMQL-Submit CLI options because the serialization of DAG Scala code generated
        // from the web interface, was issued errors when deserialized with this package. (web interface is java)
        //TODO: replace this double compile with serialized version of the DAG.
        inputs = extractDSDir(args(i + 1))
        logger.info("inputs set to: \n" + (args(i + 1).split(",")).mkString("\n"))
      } else if ("-outputDirs".equals(args(i))) {
        outputs = extractDSDir(args(i + 1))
        logger.info("outputs set to: \n" + (outputs).mkString("\n"))
      } else if ("-schemata".equals(args(i).toLowerCase())) {//List of [NAME:::schema] separated by comma
        //Input datasets Schemata can be sent from the Server Manager a string separated by :::
        schemata = extractInDSsSchema( args(i + 1))
        logger.info("Schema set to: " + args(i + 1))
      } else if ("-dag".equals(args(i).toLowerCase())) {
        // GMQLSubmit sent directly the DAG encoded as a string
        val serializedDag = args(i+1)
        if (!serializedDag.isEmpty) {
          // Deserialization of the DAG string to List[IRVariable]
          dag = Some(DAGSerializer.deserializeDAG(serializedDag))
          logger.info("A DAG was received SUCCESSFULLY")
        }
      } else if ("-dagpath".equals(args(i).toLowerCase())) {
        dagPath = args(i + 1)
        val serializedDag = readFile(dagPath)
        dag = Some(DAGSerializer.deserializeDAG(serializedDag))
        logger.info("DAG path set to: " + dagPath)
      } else {
        logger.warn("( "+ args(i) + " ) NOT A VALID COMMAND ... ")

      }
    }
//    if(dagPath != null) {
//      // Deserialization of the DAG string saved in a File
//      dag = Some(Utilities.deserializeDAG(new String(Files.readAllBytes(Paths.get(dagPath)))))
//    }
    //If the Script path is not set and the script is not loaded in the options
    // and no serialized DAG was submitted, close execution.
    if (scriptPath == null && script == null && dag.isEmpty ) {
      println(usage); sys.exit(9)
    }

    // In case scriptPath is empty then set the path to test.GMQL file,
    // This is needed only to generate JOBID.
    if (scriptPath == null)
      scriptPath = "test.GMQL"

    //read GMQL script
    val query: String =
      if (dag.isDefined) ""  //If we have a serialized DAG we do not need the query
      else if (script != null) script
      else readScriptFile(scriptPath)

    //Generate JOBID in case there is no JOBID set in the CLI options.
    if (jobid == "") jobid = generateJobId(scriptPath, username)

    //Set the logging file of this job to be stored either to /tmp folder
    // or to the user log directory in the repository.
    setlogger(jobid, verbose,if(logDir!=null)logDir else SYSTEM_TMPE_DIR)

    logger.info("Start to execute GMQL Script..")

    val implementation: Implementation = getImplemenation(executionType,jobid,outputFormat,outputCoordinateSystem)

    val server = new GmqlServer(implementation, Some(1000))
    if (dag.isDefined) {
      // If we are executing a DAG we simply add the List[IRVariable] to the
      // materialization list and execute
      server.materializationList ++= dag.get.dag
      server.run()
    }
    else {
      val translator = new Translator(server, "/tmp/")

      val translation = /*if(!dag.isEmpty) dag else translator.phase1(readScriptFile(scriptPath))*/
        compile(jobid, translator, query, inputs,outputs)

      try {
        if (translator.phase2(translation)) {
          server.run()
        }else{
          logger.error("Compile failed..")
          System.exit(0);
        }
      } catch {
        case e: CompilerException => logger.error(e.getMessage) ; System.exit(0)
        case ex:Exception => logger.error("exception: \t"+ex.getMessage);/*ex.printStackTrace();*/ System.exit(0)
        case e : Throwable =>logger.error("Throwable: "+e.getMessage);/* e.printStackTrace(); */System.exit(0)
      }
    }

  }

  def generateJobId(scriptPath: String, username: String) = "job_" + new java.io.File(scriptPath).getName.substring(0, new java.io.File(scriptPath).getName.indexOf(".")) + username + "_" + date

  def setlogger(jobId: String, verbose: Boolean, logDir:String): Unit = {
    //    org.apache.log4j.Logger.getRootLogger().getLoggerRepository().resetConfiguration();
    val fa = new FileAppender();
    fa.setName("FileLogger");
    val loggerFile = logDir +"/" +jobId.toLowerCase() + ".log"
    fa.setFile(loggerFile);
    logger.info("Logger is set to:\n" + loggerFile)
    fa.setLayout(new PatternLayout("%d %-5p [%c{1}] %m%n"));
    fa.setThreshold(Level.INFO);
    fa.setAppend(true);
    fa.activateOptions();

    //add appender to any Logger (here is root)
    org.apache.log4j.Logger.getRootLogger().addAppender(fa)
//    org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.INFO)
    org.apache.log4j.Logger.getLogger("org").setLevel(if (!verbose) org.apache.log4j.Level.WARN else org.apache.log4j.Level.INFO)
//    org.apache.log4j.Logger.getLogger("it").setLevel(if (!verbose) org.apache.log4j.Level.WARN else org.apache.log4j.Level.DEBUG)
    org.apache.log4j.Logger.getLogger("it.polimi.genomics.spark").setLevel( org.apache.log4j.Level.INFO)
//    org.apache.log4j.Logger.getLogger("it.polimi.genomics.cli").setLevel(if (!verbose) org.apache.log4j.Level.INFO else org.apache.log4j.Level.INFO)
    org.apache.log4j.Logger.getLogger("org.apache.spark").setLevel(org.apache.log4j.Level.WARN)
    org.apache.log4j.Logger.getLogger("akka").setLevel(org.apache.log4j.Level.ERROR)
//    org.apache.log4j.Logger.getLogger("it.polimi.genomics.spark.implementation.GMQLSparkExecutor").setLevel(org.apache.log4j.Level.INFO)

//    val root:ch.qos.logback.classic.Logger = org.slf4j.LoggerFactory.getLogger("org").asInstanceOf[ch.qos.logback.classic.Logger];
//    root.setLevel(ch.qos.logback.classic.Level.WARN);

  }

  def readScriptFile(file: String): String = {
    try {
      scala.io.Source.fromFile(file).mkString
    } catch {
      case ex: Throwable => logger.warn("File not found"); "NOT FOUND SCRIPT FILE."
    }
  }

  def readFile(path: String) : String = {
    val conf = new Configuration();
    val pathHadoop = new org.apache.hadoop.fs.Path(path);
    val fs = FileSystem.get(pathHadoop.toUri(), conf);
    val ifS = fs.open(pathHadoop)
    scala.io.Source.fromInputStream(ifS).mkString
  }

  def compile(id: String, translator: Translator, script: String, inputs: Map[String, String],outputs: Map[String, String]): List[Operator] = {
    var operators: List[Operator] = List[Operator]()
    try {
      //compile the GMQL Code
      val languageParserOperators = translator.phase1(script)

      operators = languageParserOperators.map(_ match {
        case d: MaterializeOperator =>

          //Case when the CLI is called from the Server Manager and the repository is set in this case.
          if (outputs.nonEmpty) {
            if (!d.store_path.isEmpty) {
              val path = id + "_" + d.store_path + "/"
              logger.info(d.store_path+", generated: " + path )
              d.store_path = outputs.get(d.store_path).getOrElse(path)
              logger.info("outputs: "+outputs.mkString("\n"))
              d
            }
            else {
              val path = id + "/"
              d.store_path = outputs.get(d.store_path).get//.getOrElse(path)
              d
            }
          }
          else d
        case select: SelectOperator => val dsinput = select.input1 match {
          case p: VariablePath => VariablePath(inputs.get(p.path).getOrElse(p.path), p.parser_name);
          case p: VariableIdentifier => VariableIdentifier(inputs.get(p.IDName).getOrElse(p.IDName));
        };

          SelectOperator(select.op_pos, dsinput, select.input2,select.output,select.parameters)
          //new SelectOperator(select.op_pos, dsinput, select.input2, select.output, select.sj_condition, select.meta_condition, select.region_condition)
        case s: Operator => s
      })
    } catch {
      case e: CompilerException => logger.error(e.getMessage)
      case ex: Exception => logger.error(ex.getMessage)
    }

    operators
  }

  def getImplemenation(executionType:String,jobid:String , outputFormat: GMQLSchemaFormat.Value, outputCoordinateSystem: GMQLSchemaCoordinateSystem.Value) ={
	val projId      = "gmql-188714"

    	val credentials = "/usr/src/myapp/credentials.json"

   	val conf = new SparkConf().setAppName("Prova").setMaster("local[*]")
      	 .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.kryoserializer.buffer", "128")
     	 .set("spark.driver.allowMultipleContexts","true")
     	 .set("spark.sql.tungsten.enabled", "true")//.setMaster("local[*]")
     	 .set("spark.hadoop.google.cloud.auth.service.account.enable","true")
     	 .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials)
     	 .set("spark.hadoop.fs.gs.impl","com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
     	 .set("spark.hadoop.fs.AbstractFileSystem.gs.impl","com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
     	 .set("spark.hadoop.fs.gs.project.id",projId)

    val sc: SparkContext = new SparkContext(conf)
 
      new GMQLSparkExecutor(testingIOFormats = false, sc = sc, outputFormat = outputFormat, outputCoordinateSystem = outputCoordinateSystem)
  }

  private def extractInDSsSchema(inputSchemata:String):Map[String, String] ={

    val conf = new Configuration();

    val DSs = inputSchemata.split(",")
    if (DSs.nonEmpty) DSs.map { x =>
      val ds = x.split(":::");
      val dir = ds(0) + DEFAULT_SCHEMA_FILE
      val file = new org.apache.hadoop.fs.Path(dir);
      val fs = FileSystem.get(file.toUri(), conf);
      if (!fs.exists(file)) {
        val br = new BufferedWriter(new OutputStreamWriter(fs.create(file), "UTF-8"));
        br.write(ds(1));
        br.close();
      }
      (dir, ds(1))
    }.toMap
    else Map()
  }

  private def extractDSDir(inDS:String):Map[String, String] = {
    val DSs = inDS.split(",")
    if (DSs.nonEmpty)
      DSs.map { x =>
        val ds = x.split(":::");
        val DSdir = ds(1)
        val path = new org.apache.hadoop.fs.Path(DSdir);
        val fs = FileSystem.get(path.toUri(), new Configuration());

        if (!fs.exists(path)) {
          logger.warn(" Directory for Dataset ( " + ds(0) + " ) is not found. ");
        }

        (ds(0), DSdir)
      }.toMap
    else Map()
  }
}
