package it.polimi.genomics.manager.Debug

import java.io.File
import java.nio.file.{Files, Paths, StandardCopyOption}

import it.polimi.genomics.core._
import it.polimi.genomics.repository.{GMQLSample, Utilities => RepoUtilities}
import it.polimi.genomics.manager
import it.polimi.genomics.manager.{GMQLContext, GMQLExecute, GMQLJob, Utilities}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import collection.JavaConverters._


object Executor {


  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  val logger = LoggerFactory.getLogger(Executor.getClass)

  def go(confDir: String, datasets: List[String], query: String, queryName: String,
         username: String, resultDir:String,  cores: Long, memory: Long, cpu_freq:Float, bin_size:Long): Unit = {

    // Set the configutation folder path and get the utilities object
    RepoUtilities.confFolder = confDir
    println("repo: " + RepoUtilities.confFolder)
    val repoUtilities = RepoUtilities()
    val repository = repoUtilities.getRepository()
    val server = GMQLExecute()


    /*Utilities().SPARK_CUSTOM = scala.collection.mutable.Map(
      "spark.driver.memory" -> scala.collection.mutable.Map(GDMSUserClass.ALL -> (memory+"g")),
      "spark.driver.cores" -> scala.collection.mutable.Map(GDMSUserClass.ALL -> cores.toString),
      "spark.master" -> scala.collection.mutable.Map(GDMSUserClass.ALL -> ("local["+cores.toString+"]")),
      "spark.eventLog.dir"->scala.collection.mutable.Map(GDMSUserClass.ALL -> (resultDir+"/logs/")),
      "spark.eventLog.enabled"->scala.collection.mutable.Map(GDMSUserClass.ALL -> "true")
      )*/


    val logFolder = resultDir+"/logs/"
    new File(logFolder) mkdirs()

    val sparkConf = new SparkConf()
      .set("spark.driver.memory", "1g")
      .set("spark.driver.cores",  cores.toString)
      .set("spark.driver.cores",  cores.toString)
      .set("spark.eventLog.enabled",  "true")
      .set("spark.eventLog.dir",  resultDir+"/logs/")
      .setMaster("local["+cores.toString+"]")
      .setAppName(queryName)





    // Move the dataset on the user repo
    for(ds<-datasets) {

      println("Importing "+ds)

      //getListOfFiles(ds).map(_.getAbsolutePath).filter(_.endsWith(".gdm")).foreach(println)
      val samples = getListOfFiles(ds).map(_.getAbsolutePath).filter(_.endsWith(".gdm")).map(s=>GMQLSample(name = s, meta = s+".meta"))

      val schemaFile = ds+"/test.schema"

      val dsName = new File(ds) .getName

      // Remove any dataset with the same name first
      try {
        repository.deleteDS(dsName, username)
      } catch {
        case e: Exception => {println("DS was not already loaded in the repository.")}
      }

      // Import the dataset in the repository
      repository.importDs(dsName,username,GDMSUserClass.ADMIN,samples.asJava, schemaFile)

    }

    val gmqlScript = GMQLScript(query, queryName)

    val sc = new SparkContext(sparkConf)

    val gmqlContext = GMQLContext(ImplementationPlatform.SPARK, repository, GMQLSchemaFormat.TAB,
      username = "public", userClass = GDMSUserClass.ADMIN, sc=sc, binSize = BinSize(bin_size,bin_size,bin_size))
    val compilationJob = new GMQLJob(gmqlContext, gmqlScript, "public")

    var jobID = compilationJob.jobId

    var skip = false


    compilationJob.compile()
    if (compilationJob.getJobStatus == manager.Status.COMPILE_FAILED) {
      logger.error("FAILED COMPILATION")
      skip = true
    } else {
      logger.info("COMPILATION SUCCESS")
      val executionJob = server.registerJob(gmqlScript, gmqlContext, "")
      jobID = executionJob.jobId
      logger.info("EXECUTING THE QUERY")
      server.execute(executionJob)
      // Wait
      do {
        logger.info("Waiting for completion")
        Thread.sleep(1000)
      } while (executionJob.getJobStatus != manager.Status.SUCCESS &&
         executionJob.getJobStatus != manager.Status.EXEC_FAILED &&
        executionJob.getJobStatus != manager.Status.EXEC_STOPPED &&
        executionJob.getJobStatus != manager.Status.DS_CREATION_FAILED)

      if(executionJob.getJobStatus != manager.Status.SUCCESS)
        skip = true
    }

    sc.stop()

    val dagFolder = RepoUtilities().getDDagDir(username)
    var fileList: List[File] = getListOfFiles(dagFolder)

    if(!skip) {

      val filePath = dagFolder + "/" + jobID + ".xml"

      println("Looking for " + filePath)

      while (!(new File(filePath) exists())) {
        println("DAG not found retrying...")
        Thread.sleep(1000)
      }


      val ddagFile = new File(filePath)

      // Move results into destination folder

      println(s"DAG folder: $dagFolder")

      /*val temp  = getListOfFiles(dagFolder).filter(_.getName.contains(jobID))
    var ddagFile:File = null
    if(temp.isEmpty) { // todo: this is a bad fix
      ddagFile =
    }else {
      ddagFile = temp.head
    }*/

      println(s"JobID: $jobID")

      val destDir = resultDir + "/ddag/"
      new File(destDir).mkdirs()

      val destFile = destDir + jobID + ".xml"
      moveRenameFile(ddagFile.getAbsolutePath, destDir + jobID + ".xml")


      val add = Map("cores" -> cores.toString, "memory" -> memory.toString, "cpu_freq" -> cpu_freq.toString, "job_id" -> jobID)
      MatrixConverter.convert(destFile, 12, 123, resultDir, add)

    } else {

      logger.error("EXECUTION FAILED for datasets : "+datasets.mkString(", ")+" with "+cores.toString+"cores and "+memory.toString+"g memory.")
    }


  }

  private def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  def moveRenameFile(source: String, destination: String): Unit = {
    val path = Files.move(
      Paths.get(source),
      Paths.get(destination),
      StandardCopyOption.REPLACE_EXISTING
    )
    // could return `path`
  }

}
