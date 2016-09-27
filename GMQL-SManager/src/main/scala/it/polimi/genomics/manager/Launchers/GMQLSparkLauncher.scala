package it.polimi.genomics.manager.Launchers

import it.polimi.genomics.manager.{Status, GMQLJob}
import org.apache.spark.launcher.SparkAppHandle
import org.slf4j.LoggerFactory

/**
  * Created by abdulrahman on 04/05/16.
  */
class GMQLSparkLauncher(sparkJob:GMQLJob) extends GMQLLauncher(sparkJob){
  private final val logger = LoggerFactory.getLogger(this.getClass);
  var launcherHandler:SparkAppHandle = null
  def run(): GMQLSparkLauncher = {
    val importController = new GMQLSparkSubmit(job);
    launcherHandler = importController.runSparkJob()

//    new Thread(new Runnable {
//      def run() {
//        while(launcherHandler.getState == SparkAppHandle.State.CONNECTED ||
//          launcherHandler.getState == SparkAppHandle.State.SUBMITTED ||
//          launcherHandler.getState == SparkAppHandle.State.RUNNING  ||
//          launcherHandler.getState == SparkAppHandle.State.UNKNOWN ){ Thread.sleep(1000)}
//        logger.info("Creating dataset...",job.outputVariablesList)
//        logger.info("State: "+job.status)
////        if(job.status == Status.EXEC_SUCCESS)
////          {
////            job.createds()
////            job.status= Status.SUCCESS
////          }
////        else
////        {
////          job.status = Status.EXEC_FAILED
////          logger.warn("Job Failed, no dataset is created.")
////        }
//
//
//      }
//    }).start()

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
    launcherHandler.kill();
  }

}