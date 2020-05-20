package it.polimi.genomics.manager.Launchers

import ch.qos.logback.classic.encoder.PatternLayoutEncoder
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.{Level, LoggerContext}
import ch.qos.logback.core.FileAppender
import ch.qos.logback.core.filter.Filter
import ch.qos.logback.core.spi.FilterReply
import it.polimi.genomics.federated.{FederatedImplementation, GmqlFederatedException}
import it.polimi.genomics.manager.Exceptions.InvalidGMQLJobException
import it.polimi.genomics.manager.{GMQLJob, Status}
import it.polimi.genomics.repository.{Utilities => General_Utilities}
import org.apache.spark.launcher.SparkAppHandle
import org.slf4j.LoggerFactory
import it.polimi.genomics.manager.Utilities

/**
  * Created by abdulrahman on 04/05/16.
  */
/**
  *
  * @param sparkJob is a [[ GMQLJob]], sparkJob should contains information on running GMQL Job
  *                 such as the implementation platform and the context, along with the code and the state.
  */
class GMQLSparkLauncher(sparkJob: GMQLJob) extends GMQLLauncher(sparkJob) {

  override var applicationID: Option[String] = _

  private final val logger = LoggerFactory.getLogger(this.getClass);

  //Spark application handler
  var launcherHandler: SparkAppHandle = null

  /**
    * Run GMQL job
    *
    * @return [[ GMQLSparkLauncher]] handle
    */
  def run(): GMQLSparkLauncher = {
    val importController = new GMQLSparkSubmit(job);
    launcherHandler = importController.runSparkJob()
    this
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
    logbackLogger.setLevel(Level.ALL)
    logbackLogger.setAdditive(false)

    (ple, fileAppender)
  }

  /**
    * Return
    **/
  override def getStatus(): Status.Value = {
    if (launcherHandler != null) {
      val status = launcherHandler.getState
      status match {
        case SparkAppHandle.State.CONNECTED => Status.PENDING
        case SparkAppHandle.State.FAILED => Status.EXEC_FAILED
        case SparkAppHandle.State.FINISHED => Status.EXEC_SUCCESS
        case SparkAppHandle.State.KILLED => Status.EXEC_STOPPED
        case SparkAppHandle.State.SUBMITTED => Status.PENDING
        case SparkAppHandle.State.RUNNING => Status.RUNNING
        case _ => Status.PENDING
      }
    }
    else
      job.getJobStatus
  }

  override def getAppName(): String = {
    applicationID = Some(launcherHandler.getAppId)
    applicationID.get
  }

  //TODO: GMQL job should be killed in YARN environment too.
  // This needs to be added bellow by calling appropriate function to kill YARN job.
  // Other wise the kill button will stop GMQL job only from the Server Manager and not from holding the resources.
  /**
    *
    * kill GMQL Job by stoping the spark context
    *
    *
    */
  override def killJob() = {
    try {
      launcherHandler.stop()
      while (launcherHandler.getState != SparkAppHandle.State.KILLED)
        Thread.sleep(500)

      launcherHandler.kill()


    } catch {
      case e: IllegalStateException =>
        e.getMessage() match {
          case "Disconnected." => throw new InvalidGMQLJobException("Job is disconnected.")
          case "Application is still not connected." => throw new InvalidGMQLJobException("Application is still not connected, please try again later.")
        }
    }
  }

  /**
    * get the log of the execution of GMQL job running using this launcher
    *
    * @return List[String] as the log of the execution
    */
  override def getLog(): List[String] = {
    import scala.io.Source
    Source.fromFile(job.userLoggerPath).getLines().toList

  }
}