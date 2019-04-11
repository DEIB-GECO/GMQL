package it.polimi.genomics.manager.Launchers

import it.polimi.genomics.core.DAG.DAGSerializer
import it.polimi.genomics.federated.{FederatedImplementation, GmqlFederatedException}
import it.polimi.genomics.manager.{GMQLJob, Status}
import it.polimi.genomics.repository.FSRepository.FS_Utilities
import it.polimi.genomics.repository.{Utilities => General_Utilities}
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.{FileAppender, Level, PatternLayout}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory


/**
  * Created by abdulrahman on 04/05/16.
  */
class GMQLLocalLauncher(localJob: GMQLJob) extends GMQLLauncher(localJob) {

  override var applicationID: Option[String] = None

  private final val logger = LoggerFactory.getLogger(this.getClass);
  var launcherHandler: GMQLJob = job

  def readFile(path: String): String = {
    val conf = FS_Utilities.gethdfsConfiguration()
    val pathHadoop = new org.apache.hadoop.fs.Path(path)
    val fs = FileSystem.get(pathHadoop.toUri(), conf)
    val ifS = fs.open(pathHadoop)
    scala.io.Source.fromInputStream(ifS).mkString
  }

  def setlogger(jobId: String, verbose: Boolean, logDir: String) = {
    //    org.apache.log4j.Logger.getRootLogger().getLoggerRepository().resetConfiguration();
    val fa = new FileAppender();
    fa.setName("FileLogger");
    val loggerFile = logDir + "/" + jobId.toLowerCase() + ".log"
    fa.setFile(loggerFile);
    logger.info("Logger is set to:\n" + loggerFile)
    fa.setLayout(new PatternLayout("%d %-5p [%c{1}] %m%n"));
    fa.setThreshold(Level.ALL);
    fa.setAppend(true);
    fa.activateOptions();
    fa.setImmediateFlush(true)

    //add appender to any Logger (here is root)
    org.apache.log4j.Logger.getRootLogger().addAppender(fa)
    //    org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.INFO)
    org.apache.log4j.Logger.getLogger("org").setLevel(if (!verbose) org.apache.log4j.Level.ALL else org.apache.log4j.Level.ALL)
    //    org.apache.log4j.Logger.getLogger("it").setLevel(if (!verbose) org.apache.log4j.Level.WARN else org.apache.log4j.Level.DEBUG)
    org.apache.log4j.Logger.getLogger("it.polimi.genomics.spark").setLevel(org.apache.log4j.Level.ALL)
    org.apache.log4j.Logger.getLogger("it.polimi.genomics.federated").setLevel(org.apache.log4j.Level.ALL)
    //    org.apache.log4j.Logger.getLogger("it.polimi.genomics.cli").setLevel(if (!verbose) org.apache.log4j.Level.INFO else org.apache.log4j.Level.INFO)
    org.apache.log4j.Logger.getLogger("org.apache.spark").setLevel(org.apache.log4j.Level.ALL)
    org.apache.log4j.Logger.getLogger("akka").setLevel(org.apache.log4j.Level.ALL)
    //    org.apache.log4j.Logger.getLogger("it.polimi.genomics.spark.implementation.GMQLSparkExecutor").setLevel(org.apache.log4j.Level.INFO)

    //    val root:ch.qos.logback.classic.Logger = org.slf4j.LoggerFactory.getLogger("org").asInstanceOf[ch.qos.logback.classic.Logger];
    //    root.setLevel(ch.qos.logback.classic.Level.WARN);

    fa
  }

  /**
    * Run GMQL job localy, this will call the programatical API of Spark/Flink,
    * No extra provcess will be created, Spark/Flink does not need to be installed in the system.
    *
    **/
  def run(): GMQLLocalLauncher = {
    new Thread(new Runnable {
      def run() {

        val fa = setlogger(job.jobId, false, General_Utilities().getUserLogDir(job.username))

        if (job.federated) {
          val serializedDag = readFile(job.script.dagPath)
          val dag = Some(DAGSerializer.deserializeDAG(serializedDag))
          job.server.materializationList ++= dag.get.dag

          job.server.implementation = new GMQLSparkExecutor(
            binSize = job.gMQLContext.binSize,
            outputFormat = job.gMQLContext.outputFormat,
            outputCoordinateSystem = job.gMQLContext.outputCoordinateSystem,
            sc = SparkContext.getOrCreate(new SparkConf().setAppName(job.jobId).setMaster("local[*]")),
            stopContext = false)
        }
        else {
          val tempDir: String =
            if (General_Utilities().GMQL_REPO_TYPE == General_Utilities().HDFS) {
              General_Utilities().getHDFSNameSpace() + General_Utilities().getResultDir("federated")
            }
            else {
              General_Utilities().getResultDir("federated")
            }
          job.server.implementation = new FederatedImplementation(Some(tempDir), Some(job.jobId))
        }
        //      new GMQLSparkExecutor(
        //      binSize = job.gMQLContext.binSize,
        //      outputFormat = job.gMQLContext.outputFormat,
        //      outputCoordinateSystem = job.gMQLContext.outputCoordinateSystem,
        //      sc = new SparkContext(new SparkConf().setAppName(job.jobId).setMaster("local[*]")))

        job.status = Status.RUNNING
        logger.info(String.format("Job %s is under execution.. ", job.jobId))
        try {
          job.server.run()
          job.status = Status.EXEC_SUCCESS
        } catch {
          case _: GmqlFederatedException => job.status = Status.EXEC_FAILED
        }
        fa.getImmediateFlush()
        fa.close()

      }
    }).start()
    this
  }

  /**
    * @return GMQL job status
    **/
  def getStatus(): Status.Value = {
    launcherHandler.status
  }

  /**
    *
    * @return String of the application name
    */
  def getAppName(): String = {
    applicationID = Some(launcherHandler.jobId)
    applicationID.get
  }

  /**
    * Kill GMQL application by stopping the implementation context
    */
  override def killJob() = launcherHandler.gMQLContext.implementation.stop()

  /** get the log of the execution of GMQL job running using this launcher
    *
    * @return [[String]] as the log of the execution
    */
  override def getLog(): List[String] = {
    import scala.io.Source
    Source.fromFile(job.userLoggerPath).getLines().toList

  }
}
