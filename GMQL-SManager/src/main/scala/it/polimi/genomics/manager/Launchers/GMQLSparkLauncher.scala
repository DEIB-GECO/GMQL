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
  * @param sparkJob is a [[ GMQLJob]], sparkJob should contains information on running GMQL Job
  *      such as the implementation platform and the context, along with the code and the state.
  */
class GMQLSparkLauncher(sparkJob:GMQLJob) extends GMQLLauncher(sparkJob){

  private final val logger = LoggerFactory.getLogger(this.getClass);

  //Spark application handler
  var launcherHandler:SparkAppHandle = null

  /**
    * Run GMQL job
    * @return [[ GMQLSparkLauncher]] handle
    */
  def run(): GMQLSparkLauncher = {
    val importController = new GMQLSparkSubmit(job);
    launcherHandler = importController.runSparkJob()
    this
  }

  /**
    * Return
    *     */
  override def getStatus(): Status.Value={
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

  override def getAppName (): String =
  {
    launcherHandler.getAppId
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

  override var applicationID: Option[String] = _

  /**
    * get the log of the execution of GMQL job running using this launcher
    *
    * @return List[String] as the log of the execution
    */
  override def getLog(): List[String] = {
    import scala.io.Source
    Source.fromFile(job.loggerPath).getLines().toList

  }
}