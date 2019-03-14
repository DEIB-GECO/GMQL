package it.polimi.genomics.manager.Launchers

import it.polimi.genomics.core.DAG.DAGSerializer
import it.polimi.genomics.federated.{FederatedImplementation, GmqlFederatedException}
import it.polimi.genomics.manager.{GMQLJob, Status}
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import it.polimi.genomics.repository.{GMQLRepository, RepositoryType, Utilities => General_Utilities}


/**
  * Created by abdulrahman on 04/05/16.
  */
class GMQLLocalLauncher(localJob: GMQLJob) extends GMQLLauncher(localJob) {

  override var applicationID: Option[String] = None

  private final val logger = LoggerFactory.getLogger(this.getClass);
  var launcherHandler: GMQLJob = job

  def readFile(path: String): String = {
    val conf = new Configuration();
    val pathHadoop = new org.apache.hadoop.fs.Path(path);
    val fs = FileSystem.get(pathHadoop.toUri(), conf);
    val ifS = fs.open(pathHadoop)
    scala.io.Source.fromInputStream(ifS).mkString
  }

  /**
    * Run GMQL job localy, this will call the programatical API of Spark/Flink,
    * No extra provcess will be created, Spark/Flink does not need to be installed in the system.
    *
    **/
  def run(): GMQLLocalLauncher = {
    new Thread(new Runnable {
      def run() {

        if (job.federated) {
          val serializedDag = readFile(job.script.dagPath)
          val dag = Some(DAGSerializer.deserializeDAG(serializedDag))
          job.server.materializationList ++= dag.get.dag

          job.server.implementation = new GMQLSparkExecutor(
            binSize = job.gMQLContext.binSize,
            outputFormat = job.gMQLContext.outputFormat,
            outputCoordinateSystem = job.gMQLContext.outputCoordinateSystem,
            sc = SparkContext.getOrCreate(new SparkConf().setAppName(job.jobId).setMaster("local[*]")),
            stopContext = false)
        }
        else {
          val tempDir: String = General_Utilities().getResultDir("federated")
          job.server.implementation = new FederatedImplementation(Some(tempDir), Some(job.jobId))
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
        }catch {
          case _:GmqlFederatedException => job.status = Status.EXEC_FAILED
        }

      }}).start()
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
