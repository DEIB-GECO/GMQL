package it.polimi.genomics.manager.Launchers

import it.polimi.genomics.manager.{GMQLJob, Status}
import it.polimi.genomics.repository.Utilities
import play.api.libs.json.{JsArray, JsString}


/**
  * Created by abdulrahman on 26/05/16.
  * Edited by andreagulino on 20/03/17.
  */
class GMQLRemoteLauncher(job:GMQLJob) extends GMQLLauncher(job){


  val outDir = job.outputVariablesList.map{x=>
    val dir = Utilities().getHDFSNameSpace() +
      Utilities().getHDFSRegionDir(job.username)+x+"/";
    x.substring(job.jobId.length+1)+":::"+dir
  }.mkString(",")


  val cliArgs =  JsArray( Seq(
      JsString("-script")      , JsString(job.script.script),
      JsString("-scriptpath")  , JsString(job.script.scriptPath),
      JsString("-inputDirs")   , JsString(job.inputDataSets.map(x => x._1+":::"+x._2+"/").mkString(",")),
      JsString("-jobid")       , JsString(job.jobId),
      JsString("-outputDirs")  , JsString(outDir),
      JsString("-outputFormat"),JsString(job.gMQLContext.outputFormat.toString),
      JsString("-outputCoordinateSystem"), JsString(job.gMQLContext.outputCoordinateSystem.toString),
      JsString("-username")    , JsString(job.username)
    ))


  val livyClient = new LivyClient(job.jobId, cliArgs, outDir)

  override var applicationID: Option[String] = _

  /**
    *
    * Run GMQL Job and return the handle to this execution
    *
    * @return [[ GMQLLauncher]]
    */
  override def run(): GMQLLauncher = {
    livyClient.run()
    this
  }

  /**
    *
    * return the Status of the job
    *
    * @return The [[ GMQLJob]] Status [[ Status]]
    */
  override def getStatus(): _root_.it.polimi.genomics.manager.Status.Value = {
    stateAdapter(livyClient.getStatus())
  }

  /**
    *
    * return the [[ GMQLJob]] Application name
    *
    * @return String of the application name
    */
  override def getAppName(): String = {
    applicationID = Some(livyClient.getAppName())
    applicationID.get
  }

  /**
    *
    * Kill GMQL Job [[ GMQLJob]]
    *
    */
  override def killJob(): Unit = {
    livyClient.killJob()
  }

  /**
    * get the log of the execution of GMQL job running using this launcher
    *
    * @return List[String] as the log of the execution
    */
  override def getLog(): List[String] = {
    livyClient.getLog()
  }

  /**
    *
    * Map the status recieved from Livy to GMQLJob Status
    *
    * @param state String from Livy
    * @return [[ Status]] of GMQL job
    */
  private def stateAdapter(state:String): Status.Value ={
    state match {
      case "\"pending\"" => Status.PENDING
      case "\"running\"" => Status.RUNNING
      case "\"success\"" => Status.EXEC_SUCCESS
      case "\"starting\"" => Status.PENDING
      case _ => Status.EXEC_FAILED
    }
  }
}
