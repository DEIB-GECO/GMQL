package it.polimi.genomics.cli

import java.io._
import java.text.SimpleDateFormat
import java.util.Date

import com.sun.jersey.core.util.Base64
import it.polimi.genomics.GMQLServer.{GmqlServer, Implementation}
import it.polimi.genomics.compiler._
import it.polimi.genomics.core.{GMQLOutputFormat, ImplementationPlatform}
import it.polimi.genomics.flink.FlinkImplementation.FlinkImplementation
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem}
import org.apache.log4j.{FileAppender, Level, PatternLayout}
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerApplicationStart, SparkListenerStageCompleted}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}
;

/**
  * Created by Abdulrahman Kaitoua on 10/09/15.
  * Email: abdulrahman.kaitoua@polimi.it
  *
  */
object GMQLExecuteCommand {
  private final val logger = LoggerFactory.getLogger(/*Logger.ROOT_LOGGER_NAME)*/ GMQLExecuteCommand.getClass);

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
    var outputFormat = GMQLOutputFormat.TAB
    var schemata = Map[String, String]()
    var inputs = Map[String, String]()
    var outputs = Map[String, String]()
    var logDir:String = null
    var verbose = false
    var i = 0;

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
          return 0
        };
        scriptPath = sFile.getPath
        logger.info("scriptpath set to: " + scriptPath)
      } else if ("-logDir".equals(args(i))) {
        logDir = args(i + 1)
        logger.info("Log Directory is set to: " + logDir)

      } else if ("-outputformat".equals(args(i).toLowerCase())) {
        val out = args(i + 1).toUpperCase().trim
        outputFormat =
          if(out == GMQLOutputFormat.TAB.toString)
            GMQLOutputFormat.TAB
          else if(out == GMQLOutputFormat.GTF.toString)
            GMQLOutputFormat.GTF
          else {
            logger.warn(s"Not knwon format $out, Setting the output format for ${GMQLOutputFormat.TAB}")
            GMQLOutputFormat.TAB
          }
        logger.info(s"Output Format set to: $out" + outputFormat)

      } else if ("-inputDirs".equals(args(i))) {
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

      }else {
        logger.warn("( "+ args(i) + " ) NOT A VALID COMMAND ... ")

      }
    }

    //If the Script path is not set and the script is not loaded in the options, close execution.
    if (scriptPath == null && script == null) {
      println(usage); sys.exit()
    }

    // In case scriptPath is empty then set the path to test.GMQL file,
    // This is needed only to generate JOBID.
    if (scriptPath == null) scriptPath = "test.GMQL"

    //read GMQL script
    val dag: String =
      if (script != null) script /*deSerializeDAG(script)*/
      else readScriptFile(scriptPath) /*List[Operator]()*/


    //Generate JOBID in case there is no JOBID set in the CLI options.
    if (jobid == "") jobid = generateJobId(scriptPath, username)

    //Set the logging file of this job to be stored either to /tmp folder
    // or to the user log directory in the repository.
    setlogger(jobid, verbose,if(logDir!=null)logDir else SYSTEM_TMPE_DIR)

    logger.info("Start to execute GMQL Script..")

    val implementation: Implementation = getImplemenation(executionType,jobid,outputFormat)

    val server = new GmqlServer(implementation, Some(1000))

    val translator = new Translator(server, "/tmp/")

    val translation = /*if(!dag.isEmpty) dag else translator.phase1(readScriptFile(scriptPath))*/
      compile(jobid, translator, dag, inputs,outputs)

    try {
      if (translator.phase2(translation)) {
        server.run()
      }else{
        logger.error("Compile failed..")
        exit(0);
      }
    } catch {
      case e: CompilerException => logger.error(e.getMessage) ; exit(0)
      case ex:Exception => logger.error("exception: \t"+ex.getMessage);/*ex.printStackTrace();*/ exit(0)
      case e : Throwable =>logger.error("Throwable: "+e.getMessage);/* e.printStackTrace(); */exit(0)
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
//    org.apache.log4j.Logger.getLogger("it.polimi.genomics.spark.implementation.MetaOperators.SelectMeta.SelectIMDWithNoIndex").setLevel( org.apache.log4j.Level.DEBUG)
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


  //TODO: Enable serialization of the DAG
  //  def deSerializeDAG(dag:String): List[Operator] ={
  //    val bytes = Base64.decode(dag.getBytes());
  //    val objectInputStream1 = new ObjectInputStream( new ByteArrayInputStream(bytes) );
  //
  //    objectInputStream1.readObject().asInstanceOf[List[Operator]];
  //  }

  def deSerializeDAG(dagEncoded: String): List[Operator] = {
    val bytes = Base64.decode(dagEncoded.getBytes());
    val objectInputStream1 = new ObjectInputStream(new ByteArrayInputStream(bytes));

    val dag = objectInputStream1.readObject().asInstanceOf[java.util.ArrayList[Operator]];

    println("hi\n" + dag.toString)
    import scala.collection.JavaConverters._
    var DAG: List[Operator] = List[Operator]()
    for (i <- 0 to dag.size() - 1) {
      DAG = DAG :+ dag.get(i)
    }

    DAG
  }

  def compile(id: String, translator: Translator, script: String, inputs: Map[String, String],outputs: Map[String, String]): List[Operator] = {
    var operators: List[Operator] = List[Operator]()
    try {
      //compile the GMQL Code
      val languageParserOperators = translator.phase1(script)

      operators = languageParserOperators.map(x => x match {
        case d: MaterializeOperator =>

          //Case when the CLI is called from the Server Manager and the repository is set in this case.
          if (outputs.size > 0) {
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
          case p: VariablePath => new VariablePath(inputs.get(p.path).getOrElse(p.path), p.parser_name);
          case p: VariableIdentifier => new VariableIdentifier(inputs.get(p.IDName).getOrElse(p.IDName));
        };

          new SelectOperator(select.op_pos, dsinput, select.input2,select.output,select.parameters)
          //new SelectOperator(select.op_pos, dsinput, select.input2, select.output, select.sj_condition, select.meta_condition, select.region_condition)
        case s: Operator => s
      })
    } catch {
      case e: CompilerException => logger.error(e.getMessage)
      case ex: Exception => logger.error(ex.getMessage)
    }

    operators
  }

  def getImplemenation(executionType:String,jobid:String , outputFormat: GMQLOutputFormat.Value) ={
    if (executionType.equals(it.polimi.genomics.core.ImplementationPlatform.SPARK.toString.toLowerCase())) {
      val conf = new SparkConf().setAppName("GMQL V2.1 Spark " + jobid)
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.kryoserializer.buffer", "64")
        .set("spark.driver.allowMultipleContexts","true")
        .set("spark.sql.tungsten.enabled", "true")//.setMaster("local[*]")
      val sc: SparkContext = new SparkContext(conf)
      sc.addSparkListener(new SparkListener() {
        override def onApplicationStart(applicationStart: SparkListenerApplicationStart) {
          logger.debug("Spark ApplicationStart: " + applicationStart.appName);
        }

        override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
          logger.debug("Number of tasks "+stageCompleted.stageInfo.numTasks+ "\tinfor:"+ stageCompleted.stageInfo.rddInfos.mkString("\n"))
          logger.debug("Spark Stage ended: " +stageCompleted.stageInfo.name+
            /*", with details ("+ stageCompleted.stageInfo.details+*/
            " ,execTime: "+ stageCompleted.stageInfo.completionTime.getOrElse(0));
        }

        override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) {
          logger.debug("Spark ApplicationEnd: " + applicationEnd.time);
        }

      });
      new GMQLSparkExecutor(testingIOFormats = false, sc = sc, outputFormat = outputFormat)
    } else /*if(executionType.equals(FLINK)) */ {
      new FlinkImplementation()
    }
  }

  private def extractInDSsSchema(inputSchemata:String):Map[String, String] ={

    val conf = new Configuration();

    val DSs = inputSchemata.split(",")
    if (!DSs.isEmpty) DSs.map { x =>
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
    if (!DSs.isEmpty)
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
