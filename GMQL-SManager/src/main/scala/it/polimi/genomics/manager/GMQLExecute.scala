package it.polimi.genomics.manager


import java.text.SimpleDateFormat
import java.util
import java.util.Date
import java.util.concurrent.{TimeUnit, Executors}
import it.polimi.genomics.flink.FlinkImplementation.FlinkImplementation
import it.polimi.genomics.manager.Launchers.{GMQLLauncher, GMQLLivyLauncher, GMQLSparkLauncher}
import it.polimi.genomics.repository.GMQLRepository.GMQLRepository
import it.polimi.genomics.repository.util.Utilities
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.collection.immutable.HashMap
import scala.collection.JavaConverters._
/**
 * Created by Abdulrahman Kaitoua on 10/09/15.
 * Email: abdulrahman.kaitoua@polimi.it
 *
 */
class GMQLExecute (){

  private final val logger = LoggerFactory.getLogger(this.getClass);
  private final val NUM_THREADS = 5;
  private final val execService = Executors.newFixedThreadPool(NUM_THREADS);
  private var id_to_job_map:Map[String,GMQLJob] = new HashMap;
  private var user_to_jobs_map:Map[String, List[String]] = new HashMap;
  private var jobid_to_ds_map:Map[String, List[String]] = new HashMap;

  def registerJob(scriptPath:String, script:String,executionType:String,binSize:Long, username:String,jobid:String = "",sc:SparkContext = null,GTFoutput:Boolean = false): GMQLJob ={

    logger.info("Execution Platform is set to "+executionType)
    logger.info("execType = \t"+executionType+",scriptPath = \t"+scriptPath+",username = \t"+username)

    val job = if(executionType.toLowerCase() == GMQLExecute.SPARK){
      new GMQLJob(new GMQLSparkExecutor(sc=sc,GTFoutput = GTFoutput),binSize,scriptPath,script,username,outputFormat = if(GTFoutput) "gtf" else "tab")
    }else /*if(executionType.toLowerCase() == GMQLExecute.FLINK)*/{
      new GMQLJob(new FlinkImplementation(),binSize,scriptPath,script,username,outputFormat = if(GTFoutput) "gtf" else "tab")
    }

    val jobProfile = if(jobid == "")job.compile() else job.compile(jobid)

    val uToj = user_to_jobs_map.get(username);
    val jToDSs = jobid_to_ds_map.get(jobProfile._1)

    var jobs: List[String]= if (!uToj.isDefined) List[String](); else uToj.get
    var dataSets: List[String]= if (!jToDSs.isDefined) List[String](); else jToDSs.get

    jobs ::= jobProfile._1
    dataSets :::= jobProfile._2

    user_to_jobs_map = user_to_jobs_map + (username-> jobs)
    jobid_to_ds_map = jobid_to_ds_map + (jobProfile._1 -> dataSets)

    //register the job
    id_to_job_map = id_to_job_map + (jobProfile._1 -> job);
    job;
  }

  private def getJob(jobId:String): GMQLJob ={
    val jobOption = id_to_job_map.get(jobId);
    if (!jobOption.isDefined) {
      logger.error("Job not found in the registered job list..."+jobId);
      throw new InvalidGMQLJobException(String.format("Job %s cannot be scheduled for execution: the job does not exists.", jobId));
      null
    }else jobOption.get
  }

  def scheduleGQLJobForYarn(jobId:String, launcher:GMQLLauncher, repository:GMQLRepository)={
    val job = getJob(jobId);
    try {
      logger.info(String.format("Job %s is under execution.. ", job.jobId))
      val state = job.runGMQL(jobId,launcher,repository)
      logger.info(String.format ("Job is finished execution with %s.. ",state))
    }catch {
      case ex:Throwable =>logger.error("exception in execution .. \n" + ex.getMessage); ex.printStackTrace() ; job.status = Status.EXEC_FAILED ;Thread.currentThread().interrupt();
    }
  }


  def scheduleGMQLJob(jobId:String)={

    val job = getJob(jobId);

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

  def getGMQLJob(username:String, jobId:String): GMQLJob ={

    logger.debug("queried job = "+jobId.trim)
    logger.debug ("jobs: ")
    id_to_job_map.foreach(x=>logger.debug(x._1,x._2) )

    val job = getJob(jobId.trim)
    if (!username.equals(job.username))
      throw new InvalidGMQLJobException(String.format("User %s is not allowed to trace job %s ", username, jobId));

    job
  }

  def getUserJobs(username:String):util.List[String]= {
    val jobsOption = user_to_jobs_map.get(username);
    if (!jobsOption.isDefined) {
      throw new NoJobsFoundException(String.format("No job found for user %s.", username));
      List[String]().asJava
    }else jobsOption.get.asJava
  }


  def getJobLogPath(username:String,jobID:String): String ={
    Utilities.getInstance().GMQLHOME+"/data/"+username +"/logs/"+jobID.toLowerCase()+".log"
  }

  def getJobLog(username:String,jobID:String): util.List[String] ={
    import scala.io.Source
    Source.fromFile(Utilities.getInstance().GMQLHOME+"/data/"+username +"/logs/"+jobID.toLowerCase()+".log").getLines().toList.asJava
  }

  def getJobDatasets (jobId:String): util.List[String] = {
    val datasets = jobid_to_ds_map.get(jobId)
    if(!datasets.isDefined){
      throw new NoJobsFoundException(s"No datasets for job found: $jobId");
      List[String]().asJava
    }else
      datasets.get.asJava
  }

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
  private final val SPARK = "spark"
  private final val FLINK = "flink"
  private final val SCIDB = "scidb"
  def apply(): GMQLExecute ={
    if(instance == null){instance = new GMQLExecute();}
    instance
  }
}
