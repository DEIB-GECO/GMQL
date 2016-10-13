package it.polimi.genomics.manager.Launchers

import it.polimi.genomics.manager.{Status, GMQLJob}
import org.slf4j.LoggerFactory

/**
  * Created by abdulrahman on 04/05/16.
  */
class GMQLLocalLauncher (localJob:GMQLJob)  extends GMQLLauncher(localJob){

  private final val logger = LoggerFactory.getLogger(this.getClass);
  var launcherHandler:GMQLJob = job

  def run(): GMQLLocalLauncher = {
    job.status = Status.RUNNING
    logger.info(String.format("Job %s is under execution.. ", job.jobId))
    if (!job.operators.isEmpty)
      if(job.status == Status.COMPILE_SUCCESS)
        job.server.run()
      else
        logger.error("When failed, Job Status = "+job.status)
    this
  }

  def getStatus(): Status.Value={
    launcherHandler.status
  }

  def getAppName (): String =
  {
    launcherHandler.jobId
  }

  override def killJob() = launcherHandler.implementation.stop()
}
