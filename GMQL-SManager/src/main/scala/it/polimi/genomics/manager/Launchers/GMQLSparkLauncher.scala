package it.polimi.genomics.manager.Launchers

import it.polimi.genomics.manager.Exceptions.InvalidGMQLJobException
import it.polimi.genomics.manager.{GMQLJob, Status}
import org.apache.spark.launcher.SparkAppHandle
import org.slf4j.LoggerFactory

/**
  * Created by abdulrahman on 04/05/16.
  */
/**
  *
  * @param sparkJob is a {@link GMQLJob}, sparkJob should contains information on running GMQL Job
  *      such as the implementation platform and the context, along with the code and the state.
  */
class GMQLSparkLauncher(sparkJob:GMQLJob) extends GMQLLauncher(sparkJob){

  private final val logger = LoggerFactory.getLogger(this.getClass);

  //Spark application handler
  var launcherHandler:SparkAppHandle = null

  /**
    * Run GMQL job
    * @return
    */
  def run(): GMQLSparkLauncher = {
    val importController = new GMQLSparkSubmit(job);
    launcherHandler = importController.runSparkJob()
    logger.info("Creating dataset Done...")
    this
  }

  def getStatus(): Status.Value={
    val status = launcherHandler.getState
    status match {
      case SparkAppHandle.State.CONNECTED => Status.PENDING
      case SparkAppHandle.State.FAILED => Status.EXEC_FAILED
      case SparkAppHandle.State.FINISHED => Status.EXEC_SUCCESS
      case SparkAppHandle.State.KILLED => Status.EXEC_FAILED
      case SparkAppHandle.State.SUBMITTED => Status.PENDING
      case SparkAppHandle.State.RUNNING => Status.RUNNING
      case _ => Status.PENDING
    }
  }

  def getAppName (): String =
  {
    launcherHandler.getAppId
  }

  override def killJob() =
  {
    try{
      launcherHandler.stop()
    }catch {
      case e: IllegalStateException =>
        e.getMessage() match{
          case "Disconnected." => throw new InvalidGMQLJobException("Job is disconnected.")
          case "Application is still not connected." => throw new InvalidGMQLJobException("Application is still not connected, please try again later.")
        }
    }
  }

}