package it.polimi.genomics.cli

import java.io._
import java.text.SimpleDateFormat
import java.util.Date

import com.google.common.io.Files
import com.sun.jersey.core.util.Base64
import it.polimi.genomics.GMQLServer.{GmqlServer, Implementation}
import it.polimi.genomics.compiler._
import it.polimi.genomics.flink.FlinkImplementation.FlinkImplementation
import it.polimi.genomics.repository.util.Utilities
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.hadoop.fs.Path
import org.apache.hadoop.util.Progressable
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
  private final val logger = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME) //GMQLExecuteCommand.getClass);

  val dateFormat = new SimpleDateFormat("yyyy-MM-dd");
  System.setProperty("current.date", dateFormat.format(new Date()));
  //  private final val command =
  private final val usage = "gmql " + " [-username USER] [-exec FLINK|SPARK] [-binsize BIN_SIZE] [-jobid JOB_ID] [-script G = SELECT();MATERIALIZE G;] [-inputs DS1:/location/on/HDFS/,DS2:/location/on/HDFS] [-verbuse] -outputFormat GTF|TAB -scriptpath /where/gmql/script/is "
  private final val SPARK = "spark"
  private final val FLINK = "flink"
  private final val SCIDB = "scidb"
  private final val GTF = "gtf"
  private final val TAB = "tab"

  def main(args: Array[String]) {
    Utilities.getInstance()
    println(args.mkString("\t"))
    logger.info("Try to execute GMQL Script..")

    var executionType = "spark";
    var bin = 5000l;
    var scriptPath: String = null;
    //String.empty
    var script: String = null
    var username: String = null
    var outputPath = ""
    var jobid = ""
    var outputFormat = TAB
    var schema = Map[String, String]()
    var inputs = Map[String, String]()
    var inDS:String = null
    var inSchema:String = null
    var i = 0;
    var verbuse = false
    for (i <- 0 until args.length if (i % 2 == 0)) {
      if ("-h".equals(args(i)) || "-help".equals(args(i))) {
        println(usage)
      }
      else if ("-exec".equals(args(i))) {
        executionType = args(i + 1).toLowerCase()

        logger.info("Execution Type is set to: " + executionType)
      } else if ("-inputs".equals(args(i))) {
        inDS = args(i + 1)
        logger.info("inputs set to: \n" + (inDS.split(",")).mkString("\n"))
      } else if ("-bin".equals(args(i))) {
        bin = args(i + 1).toLong
        logger.info("Bin size set to: " + bin)

      } else if ("-username".equals(args(i))) {
        username = args(i + 1).toLowerCase()

        logger.info("Username set to: " + username)
      } else if ("-jobid".equals(args(i))) {
        jobid = args(i + 1).toLowerCase()

        logger.info("JobID set to: " + jobid)
      } else if ("-outputformat".equals(args(i).toLowerCase())) {
        outputFormat = args(i + 1).toLowerCase()

        logger.info("Output Format set to: " + outputFormat)
      } else if ("-verbuse".equals(args(i).toLowerCase())) {
        verbuse = true

        logger.info("Output is set to Verbuse: " + verbuse)
      } else if ("-script".equals(args(i).toLowerCase())) {
        script = args(i + 1)

        logger.info("Output Format set to: " + outputFormat)
      } else if ("-schema".equals(args(i).toLowerCase())) {
        inSchema = args(i + 1)

        logger.info("Schema set to: " + inSchema)
      } else if ("-scriptpath".equals(args(i))) {
        //        if(new java.io.File (args(i + 1)).exists()){
        scriptPath = args(i + 1)
        logger.info("scriptpath set to: " + scriptPath)
        //        }else {
        //          logger.error("GMQL script path is not valid"+args(i + 1))
        //          sys.exit()
        //        }
      }
    }

    if (username == null) {
      Utilities.getInstance(); username = Utilities.USERNAME
    } else {
      Utilities.USERNAME = username;
    }
    if (scriptPath == null && script == null) {
      println(usage); sys.exit()
    }
    if (scriptPath == null) scriptPath = "test.GMQL"
    val dag = if (script != null) script /*deSerializeDAG(script)*/ else readScriptFile(scriptPath) /*List[Operator]()*/
    if (jobid == "") jobid = generateJobId(scriptPath, username)

    if (inDS != null) {
      val DSs = inDS.split(",")
      inputs = if (!DSs.isEmpty) DSs.map { x => val ds = x.split(":::");
        val hdfs = Utilities.getInstance().gethdfsConfiguration().get("fs.defaultFS")
        val fs = Utilities.getInstance().getFileSystem
        val DSdir = hdfs + ds(1)
        println(DSdir)
        if(!fs.exists(new Path(DSdir))){logger.error("Dataset ( "+ds(0)+" ) is not found in HDFS. ");sys.exit(0);}
        (ds(0), ds(1)) }.toMap else Map()
    }

    if(inSchema!=null){
      val DSs = inSchema.split(",")
      schema = if (!DSs.isEmpty) DSs.map { x =>
        val ds = x.split(":::");
        val dir = if (ds(0).startsWith("hdfs")|| ds(0).startsWith(Utilities.getInstance().HDFSRepoDir)) ds(0) + "/test.schema"
        else {
          val hdfs = Utilities.getInstance().gethdfsConfiguration().get("fs.defaultFS")
          hdfs + Utilities.getInstance().HDFSRepoDir + username + "/regions" + ds(0) + "/test.schema"
        };
        val fs = Utilities.getInstance().getFileSystem
        val file = new org.apache.hadoop.fs.Path(dir);
        if (!fs.exists(file)) {
          fs.delete(file, true);
        }
        val br = new BufferedWriter(new OutputStreamWriter(fs.create(file), "UTF-8"));
        br.write(ds(1));
        br.close();
//        fs.close();


        (dir, ds(1))
      }.toMap
      else Map()
    }
    //    org.apache.log4j.Logger.getRootLogger().getLoggerRepository().resetConfiguration();
    setlogger(jobid, verbuse)

    logger.info("Start to execute GMQL Script..")

    val implementation: Implementation = if (executionType.equals(SPARK)) {
      val sc: SparkContext = new SparkContext(new SparkConf().setAppName("GMQL V2 Spark " + jobid))
      sc.addSparkListener(new SparkListener() {
        override def onApplicationStart(applicationStart: SparkListenerApplicationStart) {
          logger.debug("Spark ApplicationStart: " + applicationStart.appName);
        }

        override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
          logger.debug("Number of tasks "+stageCompleted.stageInfo.numTasks+ "\tinfor:"+ stageCompleted.stageInfo.rddInfos.mkString("\n"))
          logger.debug("Spark Stage ended: " +stageCompleted.stageInfo.name+/*", with details ("+ stageCompleted.stageInfo.details+*/" ,execTime: "+ stageCompleted.stageInfo.completionTime.getOrElse(0));
        }

        override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) {
          logger.debug("Spark ApplicationEnd: " + applicationEnd.time);
        }

      });
      new GMQLSparkExecutor(testingIOFormats = false, sc = sc, GTFoutput = if (outputFormat.equals(GTF)) true else false)
    } else /*if(executionType.equals(FLINK)) */ {
      new FlinkImplementation()
    }

    val server = new GmqlServer(implementation, Some(1000))

    val translator = new Translator(server, "/tmp/")

    val translation = /*if(!dag.isEmpty) dag else translator.phase1(readScriptFile(scriptPath))*/ compile(jobid, translator, dag, inputs)
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

  val date = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());

  def generateJobId(scriptPath: String, username: String) = "job_" + new java.io.File(scriptPath).getName.substring(0, new java.io.File(scriptPath).getName.indexOf(".")) + username + "_" + date

  def setlogger(jobId: String, verbuse: Boolean): Unit = {
    val fa = new FileAppender();
    fa.setName("FileLogger");
    val loggerFile = Utilities.getInstance().GMQLHOME + "/data/" + Utilities.USERNAME + "/logs/" + jobId.toLowerCase() + ".log"
    fa.setFile(loggerFile);
    logger.info("Logger is set to:\n" + loggerFile)
    fa.setLayout(new PatternLayout("%d %-5p [%c{1}] %m%n"));
    fa.setThreshold(Level.INFO);
    fa.setAppend(true);
    fa.activateOptions();

    println ("hi man")
    //add appender to any Logger (here is root)
    org.apache.log4j.Logger.getRootLogger().addAppender(fa)
    org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.INFO)
    org.apache.log4j.Logger.getLogger("org").setLevel(if (!verbuse) org.apache.log4j.Level.ERROR else org.apache.log4j.Level.INFO)
//    org.apache.log4j.Logger.getLogger("it").setLevel(if (!verbuse) org.apache.log4j.Level.DEBUG else org.apache.log4j.Level.DEBUG)
//    org.apache.log4j.Logger.getLogger("it.polimi.genomics.spark.implementation.MetaOperators.SelectMeta.SelectIMDWithNoIndex").setLevel( org.apache.log4j.Level.DEBUG)
//    org.apache.log4j.Logger.getLogger("it.polimi.genomics.cli").setLevel(if (!verbuse) org.apache.log4j.Level.INFO else org.apache.log4j.Level.INFO)
    org.apache.log4j.Logger.getLogger("org.apache.spark").setLevel(org.apache.log4j.Level.ERROR)
    org.apache.log4j.Logger.getLogger("akka").setLevel(org.apache.log4j.Level.ERROR)
//    org.apache.log4j.Logger.getLogger("it.polimi.genomics.spark.implementation.GMQLSparkExecutor").setLevel(org.apache.log4j.Level.INFO)

  }

  def readScriptFile(file: String): String = {
    try {
      scala.io.Source.fromFile(file).mkString
    } catch {
      case ex: Throwable => logger.warn("File not found"); "NOT FOUND SCRIPT FILE."
    }
  }

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

  def compile(id: String, translator: Translator, script: String, inputs: Map[String, String]): List[Operator] = {
    var operators: List[Operator] = List[Operator]()
    try {
      //compile the GMQL Code
      val languageParserOperators = translator.phase1(script)

      operators = languageParserOperators.map(x => x match {
        case d: MaterializeOperator =>
          if (inputs.size > 0) {
            if (!d.store_path.isEmpty)
              {
                println(d.op_pos, id + "_" + d.store_path + "/")
                //MaterializeOperator(d.op_pos, id + "_" + d.store_path + "/", d.input1, d.input2)
                d.store_path = id + "_" + d.store_path + "/"
                d
              };
            else
              {
                println(d.op_pos, id + "/", d.input1)
                //MaterializeOperator(d.op_pos, id + "/", d.input1, d.input2)
                d.store_path = id + "/"
                d
              };
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
}
