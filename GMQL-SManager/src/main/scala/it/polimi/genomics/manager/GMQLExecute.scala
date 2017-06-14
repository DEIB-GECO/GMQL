package it.polimi.genomics.manager


import java.io.{File, PrintWriter}
import java.util
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}

import it.polimi.genomics.core.{BinSize, GMQLSchemaFormat, GMQLScript, ImplementationPlatform}
import it.polimi.genomics.manager.Exceptions.{InvalidGMQLJobException, NoJobsFoundException}
import it.polimi.genomics.manager.Launchers.{GMQLLauncher, GMQLLocalLauncher, GMQLRemoteLauncher, GMQLSparkLauncher}
import it.polimi.genomics.repository.{Utilities => General_Utilities}
import org.apache.spark.SparkContext
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.immutable.HashMap
/**
 * Created by Abdulrahman Kaitoua on 10/09/15.
 * Email: abdulrahman.kaitoua@polimi.it
 *
 */
class GMQLExecute (){

  private final val logger: Logger = LoggerFactory.getLogger(this.getClass);
  private final val NUM_THREADS: Int = 5;
  private final val execService: ExecutorService = Executors.newFixedThreadPool(NUM_THREADS);
  private var JOBID_TO_JOB_INSTANCE:Map[String, GMQLJob] = new HashMap;
  private var USER_TO_JOBID:Map[String, List[String]] = new HashMap;
  private var JOBID_TO_OUT_DSs:Map[String, List[String]] = new HashMap;

  /**
    * GMQL job is registered in the list of the jobs of for a specific user.
    * This includes:
    *   - Compilation of the job
    *   - Register the user in the history list (if not registered yet)
    *   - Register the jobID with to the user
    *
    * @param script [[ GMQLScript]], contains script string in addition to the script path. script path is used in generating the jobID.
    * @param gMQLContext [[ GMQLContext]] contains job running environment
    * @param jobid [[ BinSize]] contains the defaults for the job binning parameters
    * @return [[ GMQLJob]] contains all the information of the job.
    */
  def registerJob(script:GMQLScript, gMQLContext: GMQLContext, jobid:String = ""): GMQLJob ={
    def saveScript(fileName:String) = {
      val scriptHistory: File = new File(General_Utilities().getScriptsDir(gMQLContext.username)+ fileName + ".gmql")
      new PrintWriter(scriptHistory) { write(script.script); close }
    }


    logger.info("Execution Platform is set to "+gMQLContext.implPlatform+"\n\tScriptPath = "+script.scriptPath+"\n\tUsername = "+gMQLContext.username)

    val job: GMQLJob = new GMQLJob(gMQLContext,script,gMQLContext.username)

    //query script of the job
    saveScript(job.jobId)

    val jobProfile: (String, List[String]) = if(jobid == "")job.compile() else job.compile(jobid)
    val jID: String = jobProfile._1
    val outDSs: List[String] = jobProfile._2

    //query script of the dataset
    outDSs.foreach(saveScript)

    val uToj: Option[List[String]] = USER_TO_JOBID.get(gMQLContext.username);
    val jToDSs: Option[List[String]] = JOBID_TO_OUT_DSs.get(jID)

    //if the user is not found create empty lists of jobs and out Datasets.
    var jobs: List[String]= if (!uToj.isDefined) List[String](); else uToj.get
    var dataSets: List[String]= if (!jToDSs.isDefined) List[String](); else jToDSs.get

    jobs ::= jID
    dataSets :::= outDSs

    USER_TO_JOBID = USER_TO_JOBID + (gMQLContext.username-> jobs)
    JOBID_TO_OUT_DSs = JOBID_TO_OUT_DSs + (jID -> dataSets)

    //register the job
    JOBID_TO_JOB_INSTANCE = JOBID_TO_JOB_INSTANCE + (jID -> job);
    job;
  }

  /**
    * Get the job instance from the job name.
    * If the job is not found  [[ InvalidGMQLJobException]] exception will be thrown
    *
    * @param jobId [[ String]] describe the GMQL job name (ID)
    * @return [[ GMQLJob]] instance
    */
  private def getJob(jobId:String): GMQLJob ={
    val jobOption = JOBID_TO_JOB_INSTANCE.get(jobId);
    if (!jobOption.isDefined) {
      logger.error("Job not found in the registered job list..."+jobId);
      throw new InvalidGMQLJobException(String.format("Job %s cannot be scheduled for execution: the job does not exists.", jobId));
      null
    }else jobOption.get
  }

  /**
    * Try to Execute GMQL Job. The job will be checked for execution of the provided platform
    * and run in case of clear from errors.
    *
    * @param jobId [[ String]] as the JobID.
    * @param launcher There is a set of launchers that implements [[ GMQLLauncher]].
    */
  @deprecated
  def execute(jobId:String, launcher:GMQLLauncher)={
    val job = getJob(jobId);
    try {
      logger.info(String.format("Job %s is under execution.. ", job.jobId))
      val state = job.runGMQL(jobId,launcher)
      logger.info(String.format ("Job is finished execution with %s.. ",state))
    }catch {
      case ex:Throwable =>logger.error("exception in execution .. \n" + ex.getMessage); ex.printStackTrace() ; job.status = Status.EXEC_FAILED ;Thread.currentThread().interrupt();
    }
  }


  /*
  **
  * Try to Execute GMQL Job. The job will be checked for execution of the provided platform
  * and run in case of clear from errors.
  *
  * @param jobId [[ String]] as the JobID.
    * @param launcher There is a set of launchers that implements [[ GMQLLauncher]].
  */
  def execute(job:GMQLJob):Unit={
    val launcher_mode = Utilities().LAUNCHER_MODE

    val launcher: GMQLLauncher =

      if ( launcher_mode equals Utilities().CLUSTER_LAUNCHER ) {
        logger.info("Using Spark Launcher")
        new GMQLSparkLauncher(job)
      } else
      if ( launcher_mode equals Utilities().REMOTE_CLUSTER_LAUNCHER ) {
        logger.info("Using Remote Launcher")
        new GMQLRemoteLauncher(job)
      } else
      if ( launcher_mode equals Utilities().LOCAL_LAUNCHER ) {
        logger.info("Using Local Launcher")
        new GMQLLocalLauncher(job)
        } else {
        throw new Exception("Unknown launcher mode.")
      }
    execute(job.jobId,launcher)

  }

  /**
    * This function enables the DAG to be passed to a GMQLJob as a serialized String.
    * It executes directly the DAG.
    * */
  def executeDAG(username: String, serializedDag : String, outputFormat : String): GMQLJob = {
    val outputGMQLFormat = GMQLSchemaFormat.getType(outputFormat)
    val gmqlScript = new GMQLScript("", "", serializedDag, "")
    val binsize = new BinSize(5000, 5000, 1000)
    val repository = General_Utilities().getRepository()
    val gmqlContext = new GMQLContext(repository, outputGMQLFormat)
    val job = new GMQLJob(gmqlContext, gmqlScript, username)
    /*The job id is generated with the username, the current date and the suffix "DAG".*/
    job.runGMQL(job.generateJobId(username, "DAG"), getLauncher(job))
    job
  }

  /**
    *  Gets the launcher given a GMQLJob
  * */
  def getLauncher(job:GMQLJob): GMQLLauncher = {
    val launcher_mode = Utilities().LAUNCHER_MODE
    val launcher: GMQLLauncher =

      if ( launcher_mode equals Utilities().CLUSTER_LAUNCHER ) {
        logger.info("Using Spark Launcher")
        new GMQLSparkLauncher(job)
      } else
      if ( launcher_mode equals Utilities().REMOTE_CLUSTER_LAUNCHER ) {
        logger.info("Using Remote Launcher")
        new GMQLRemoteLauncher(job)
      } else
      if ( launcher_mode equals Utilities().LOCAL_LAUNCHER ) {
        logger.info("Using Local Launcher")
        new GMQLLocalLauncher(job)
      } else {
        throw new Exception("Unknown launcher mode.")
      }
    launcher
  }


  @deprecated
  def scheduleGMQLJob(jobId:String)={

    val job: GMQLJob = getJob(jobId);

    execService.submit(new Runnable() {
      @Override
      def run() {
        try {
          logger.info(String.format ("Job %s is under execution.. ",job.jobId))
          val state  = job.runGMQL()
          logger.info(String.format ("Job %s is finished execution.. ",state))
        }catch {
          case ex:Throwable =>logger.error("exception in execution ..\n" + ex.getMessage); ex.printStackTrace(); job.status = Status.EXEC_FAILED ;Thread.currentThread().interrupt();
        }
      }
    })
  }

  /**
    *
    * retrieve GMQL job by providing the username and the job id
    *
    * @param username [[ String]] of the username (owner of the job)
    * @param jobId [[ String]] as the job id
    * @return [[ GMQLJob]] instance
    */
  def getGMQLJob(username:String, jobId:String): GMQLJob ={

    logger.debug("queried job = "+jobId.trim)
    logger.debug ("jobs: ")
    JOBID_TO_JOB_INSTANCE.foreach(x=>logger.debug(x._1,x._2) )

    val job = getJob(jobId.trim)
    if (!username.equals(job.username))
      throw new InvalidGMQLJobException(String.format("User %s is not allowed to trace job %s ", username, jobId));

    job
  }

  /**
    *  List all the jobs of the user
    *
    * @param username [[ String]] as the user name
    * @return [[ util.List]] of [[ String]] as the job ids of the requested user.
    */
  def getUserJobs(username:String):util.List[String]= {
    val jobsOption = USER_TO_JOBID.get(username);
    if (!jobsOption.isDefined) {
      throw new NoJobsFoundException(String.format("No job found for user %s.", username));
      List[String]().asJava
    }else jobsOption.get.asJava
  }

  /**
    *
    * return the job output datasets names.
    *
    * @param jobId [[ String]] as the GMQL Job id
    * @return List of Strings of the output datasets names
    */
  def getJobDatasets (jobId:String): util.List[String] = {
    val datasets = JOBID_TO_OUT_DSs.get(jobId)
    if(!datasets.isDefined){
      throw new NoJobsFoundException(s"No datasets for job found: $jobId");
      List[String]().asJava
    }else
      datasets.get.asJava
  }

  /**
    * ShutDown The Server Manager by terminating all the thread pool.
    *
    */
  def shotdown(): Unit ={
    execService.shutdown()
    try{
      execService.awaitTermination(Long.MaxValue, TimeUnit.SECONDS);
    }catch{
      case ex:InterruptedException => println (ex.getMessage)
      }
  }


}
object GMQLExecute{

  var instance:GMQLExecute= null

  def apply(): GMQLExecute ={
    if(instance == null){instance = new GMQLExecute();}
    instance
  }

  /**
    * Get the path of the log file of a specific job in a specific user
    *
    * @param username
    * @param jobID
    * @return
    */
  def getJobLogPath(username:String,jobID:String): String ={
    General_Utilities().getLogDir(username)+jobID.toLowerCase()+".log"
  }

  /**
    * return all the log strings of a specific job.
    *
    * @param username [[ String]] of the username
    * @param jobID [[ String]] of the JobID
    * @return list of Strings of the log information.
    */
  @deprecated
  def getJobLog(username:String,jobID:String): util.List[String] ={
    instance.getGMQLJob(username, jobID).getLog.asJava
  }
}

