package it.polimi.genomics.manager.Launchers

import ch.qos.logback.classic.encoder.PatternLayoutEncoder
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.{Level, LoggerContext}
import ch.qos.logback.core.FileAppender
import ch.qos.logback.core.filter.Filter
import ch.qos.logback.core.spi.FilterReply
import it.polimi.genomics.core.DAG.DAGSerializer
import it.polimi.genomics.core.GDMSUserClass
import it.polimi.genomics.federated.{FederatedImplementation, GmqlFederatedException}
import it.polimi.genomics.manager.{GMQLJob, Status, Utilities}
import it.polimi.genomics.repository.FSRepository.FS_Utilities
import it.polimi.genomics.repository.{Utilities => General_Utilities}
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.hadoop.fs.FileSystem
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


  class SampleFilter(threadName: String) extends Filter[ILoggingEvent] {
    override def decide(event: ILoggingEvent): FilterReply =
      if (event.getThreadName.equals(threadName))
        FilterReply.ACCEPT
      else
        FilterReply.DENY
  }

  private def createLoggerFor(jobId: String, verbose: Boolean, logDir: String) = {
    val loggerFile = logDir + "/" + jobId.toLowerCase() + ".log"

    val lc = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]
    val ple = new PatternLayoutEncoder
    ple.setPattern("%date %msg%n")
    ple.setContext(lc)
    ple.start()

    val fileAppender = new FileAppender[ILoggingEvent]
    fileAppender.setFile(loggerFile)
    fileAppender.setEncoder(ple)
    fileAppender.setContext(lc)
    fileAppender.addFilter(new SampleFilter(jobId))
    fileAppender.start()


    val logbackLogger = LoggerFactory.getLogger(classOf[FederatedImplementation]).asInstanceOf[ch.qos.logback.classic.Logger]
    logbackLogger.addAppender(fileAppender)
    logbackLogger.setLevel(Level.INFO)
    logbackLogger.setAdditive(false)

    (ple, fileAppender)
  }


  /**
    * Run GMQL job localy, this will call the programatical API of Spark/Flink,
    * No extra provcess will be created, Spark/Flink does not need to be installed in the system.
    *
    **/
  def run(): GMQLLocalLauncher = {
    new Thread(new Runnable {
      def run() {

        var master = "local[*]"
        var sparkConf = new SparkConf().setAppName(job.jobId).setMaster("local[*]")
        if(Utilities().SPARK_CUSTOM.keys.exists(k=>k=="spark.master")) {
          val master = Utilities().SPARK_CUSTOM("spark.master").head._2
          sparkConf = sparkConf.setMaster(master)
          println("setting spark master to "+master)
        }


        // Set user-category-dependent Spark properties, if any
        if( Utilities().SPARK_CUSTOM.nonEmpty ) {

          for( spark_property <- Utilities().SPARK_CUSTOM.keys ) {

            val userClass = job.gMQLContext.userClass
            val allClass  = GDMSUserClass.ALL
            val property  =  Utilities().SPARK_CUSTOM(spark_property)

            // Note: A property set for a specific user class overrides the same property possibly defined for all classes
            if ( property.isDefinedAt(userClass) ) {
              sparkConf = sparkConf.set(spark_property, property(userClass))
            } else if( property.isDefinedAt(allClass)  ) {
              sparkConf = sparkConf.set(spark_property, property(allClass))
            }
          }
        }

        val sc = SparkContext.getOrCreate(sparkConf)


        //val logs: (PatternLayoutEncoder, FileAppender[ILoggingEvent]) = createLoggerFor(job.jobId, false, General_Utilities().getUserLogDir(job.username))

        if (job.federated) {
          val serializedDag = readFile(job.script.dagPath)
          val dag = Some(DAGSerializer.deserializeDAG(serializedDag))
          job.server.materializationList ++= dag.get.dag

          job.server.implementation = new GMQLSparkExecutor(
            binSize = job.gMQLContext.binSize,
            outputFormat = job.gMQLContext.outputFormat,
            outputCoordinateSystem = job.gMQLContext.outputCoordinateSystem, sc=sc,
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
          //todo: why was Federated?
          job.server.implementation = new FederatedImplementation("LOCAL", Some(tempDir), Some(job.jobId))
          job.server.implementation =
                new GMQLSparkExecutor(
                binSize = job.gMQLContext.binSize,
                outputFormat = job.gMQLContext.outputFormat,
                outputCoordinateSystem = job.gMQLContext.outputCoordinateSystem, sc=job.gMQLContext.sc, profileData =  Utilities().PROFILE_DATA )
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
        //logs._2.stop()
        //logs._1.stop()


      }
    }, job.jobId).start()
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
