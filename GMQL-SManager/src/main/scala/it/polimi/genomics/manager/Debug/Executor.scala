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
import scala.xml.{Elem, Node}


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

    println( "PROFILE_DATA: "+Utilities().PROFILE_DATA)

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
      .set("spark.eventLog.enabled",  "true")
      .set("spark.ui.port",  "9988")
      .set("spark.eventLog.dir",  resultDir+"/logs/")
      .setMaster("local["+cores.toString+"]")
      .setAppName(queryName)


    // Clean the repository: warning: don't run it on a real repository
    repository.unregisterUser("public")
    repository.registerUser("public")


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

    var sc:SparkContext = if(Utilities().LAUNCHER_MODE == "LOCAL") new SparkContext(sparkConf) else null


    val gmqlContext = GMQLContext(ImplementationPlatform.SPARK, repository, GMQLSchemaFormat.TAB, sc=sc,
      username = "public", userClass = GDMSUserClass.ADMIN, binSize = BinSize(bin_size,bin_size,bin_size))
    val compilationJob = new GMQLJob(gmqlContext, gmqlScript, "public")
    print(gmqlScript);

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
        logger.info("Waiting for completion "+executionJob.getJobStatus)
        Thread.sleep(1000)
      } while (executionJob.getJobStatus == manager.Status.PENDING || executionJob.getJobStatus == manager.Status.RUNNING || executionJob.getJobStatus == manager.Status.DS_CREATION_RUNNING)

      if(executionJob.getJobStatus != manager.Status.SUCCESS &&
        executionJob.getJobStatus != manager.Status.EXEC_SUCCESS &&
        executionJob.getJobStatus != manager.Status.DS_CREATION_SUCCESS)
        skip = true
    }

    if(Utilities().LAUNCHER_MODE == "LOCAL")  sc.stop()

    val dagFolder = RepoUtilities().getDDagDir(username)
    var fileList: List[File] = getListOfFiles(dagFolder)

    if(!skip) {

      print("Query name:"+queryName)

      def getListOfFiles(dir: String):List[File] = {
        val d = new File(dir)
        if (d.exists && d.isDirectory) {
          d.listFiles.filter(_.isFile).toList
        } else {
          List[File]()
        }
      }

      val ddag_files = getListOfFiles(dagFolder)

      print("DAG FOLDER: "+dagFolder+" QUERYNAME: "+queryName)
      var ddag_full_path = ddag_files.filter(f =>
        f.getName.contains(queryName) && !f.getName.contains(".crc")
      ).head.getAbsolutePath


      //val filePath = dagFolder + "/" + compilationJob.generateResultName(queryName)+"_"+queryName+ ".ddag"

      println("Looking for " + ddag_full_path)

      while (!(new File(ddag_full_path) exists())) {
        println("DAG not found retrying...")
        Thread.sleep(1000)

        ddag_full_path = ddag_files.filter(f =>
          f.getName.contains(queryName) && !f.getName.contains(".crc")
        ).head.getAbsolutePath


        //val filePath = dagFolder + "/" + compilationJob.generateResultName(queryName)+"_"+queryName+ ".ddag"

        println("Looking for " + ddag_full_path)

      }


      val ddagFile = new File(ddag_full_path)

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

      val add = Map("cores" -> cores.toString, "memory" -> memory.toString, "cpu_freq" -> cpu_freq.toString, "job_id" -> jobID, "query" -> query)


      // Add info to XML
      import scala.xml.XML
      val xml = XML.loadFile(ddagFile.getAbsolutePath)

      def addChild(n: Node, newChild: Node) = n match {
        case Elem(prefix, label, attribs, scope, child @ _*) =>
          Elem(prefix, label, attribs, scope, child ++ newChild : _*)
        case _ => error("Can only add children to elements!")
      }

      val add_info = <execution>
        <cores>{add("cores")}</cores>
        <memory>{add("memory")}</memory>
        <cpu_freq>{add("cpu_freq")}</cpu_freq>
        <query>{add("query")}</query>
      </execution>

      val finalXML = addChild(xml, add_info)
      XML.save(ddagFile.getAbsolutePath, finalXML, "UTF-8")


      moveRenameFile(ddagFile.getAbsolutePath, destFile)


      if(Utilities().PROFILE_DATA)
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