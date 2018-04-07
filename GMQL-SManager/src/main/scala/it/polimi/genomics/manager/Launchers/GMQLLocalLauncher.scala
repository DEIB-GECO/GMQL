package it.polimi.genomics.manager.Launchers

import it.polimi.genomics.manager.{GMQLJob, Status}
import org.slf4j.LoggerFactory

/**
  * Created by abdulrahman on 04/05/16.
  */
class GMQLLocalLauncher(localJob: GMQLJob) extends GMQLLauncher(localJob) {

  override var applicationID: Option[String] = None

  private final val logger = LoggerFactory.getLogger(this.getClass);
  var launcherHandler: GMQLJob = job

  /**
    * Run GMQL job localy, this will call the programatical API of Spark/Flink,
    * No extra provcess will be created, Spark/Flink does not need to be installed in the system.
    *
    *     */
  def run(): GMQLLocalLauncher = {
    job.status = Status.RUNNING
    logger.info(String.format("Job %s is under execution.. ", job.jobId))
    job.server.run()

    job.status = Status.EXEC_SUCCESS
    this
  }

  /**
    * @return GMQL job status
    *     */
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
    *  Kill GMQL application by stopping the implementation context
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
