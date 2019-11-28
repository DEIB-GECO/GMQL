package it.polimi.genomics.manager.test

import it.polimi.genomics.core.{GDMSUserClass, GMQLSchemaFormat, GMQLScript, ImplementationPlatform}
import it.polimi.genomics.manager
import it.polimi.genomics.manager.{GMQLContext, GMQLExecute, GMQLJob}
import it.polimi.genomics.repository.{GMQLSample, Utilities}
import org.slf4j.LoggerFactory

import collection.JavaConverters._

object TestSManager extends App {

  val logger = LoggerFactory.getLogger(TestSManager.getClass)

  val confPath = "C:\\Users\\lucan\\Documents\\progetti_phd\\GMQL-WEB\\conf\\gmql_conf"
  Utilities.confFolder = confPath
  val repositoryUtilities = Utilities()
  val repository = repositoryUtilities.getRepository()

  logger.info("Repository information:")
  logger.info("RepoDir: " + repositoryUtilities.RepoDir)
  logger.info("HDFSRepoDir: " + repositoryUtilities.HDFSRepoDir)
  logger.info("MODE: " + repositoryUtilities.MODE)
  logger.info("GMQL_REPO_TYPE: " + repositoryUtilities.GMQL_REPO_TYPE)
  logger.info("GMQLHOME: " + repositoryUtilities.GMQLHOME)
  logger.info("HADOOP_HOME: " + repositoryUtilities.HADOOP_HOME)
  logger.info("GMQL_CONF_DIR: " + repositoryUtilities.GMQL_CONF_DIR)
  logger.info("GF_ENABLED: " + repositoryUtilities.GF_ENABLED)

  // User Information
  val username = "lucaProva"
  val userClass = GDMSUserClass.GUEST

  // Dataset information
  val datasetName = "datasetProva"
  val sampleList = List(
    GMQLSample("C:\\Users\\lucan\\Documents\\progetti_phd\\PyGMQL\\data\\narrow_sample\\ARID3A_0.gdm",
    "C:\\Users\\lucan\\Documents\\progetti_phd\\PyGMQL\\data\\narrow_sample\\ARID3A_0.gdm.meta")
  )
  val schemaPath = "C:\\Users\\lucan\\Documents\\progetti_phd\\PyGMQL\\data\\narrow_sample\\schema.xml"

  // Query
  val query = s"DATASET = SELECT() $datasetName;" +
    s"MATERIALIZE DATASET INTO DATASET_RESULT;"
  val queryName = "queryName"
  val gmqlScript = GMQLScript(query, queryName)
  val gmqlContext = GMQLContext(ImplementationPlatform.SPARK, repository, GMQLSchemaFormat.TAB,
    username = username, userClass = userClass)
  val server = GMQLExecute()

  // Registering the user
  logger.info("Registering user " + username + " with user class " + userClass)
  repository.registerUser(username)

  // Importing the dataset
  logger.info("Importing dataset " + datasetName)
  if(repository.DSExists(datasetName, username))
    repository.deleteDS(datasetName, username)
  else
    repository.importDs(datasetName, username, userClass, sampleList.asJava, schemaPath)

  // Query compilation
  logger.info("Compiling the query")
  val compilationJob = new GMQLJob(gmqlContext, gmqlScript, username)
  compilationJob.compile()
  if(compilationJob.getJobStatus == manager.Status.COMPILE_FAILED)
    logger.error("FAILED COMPILATION")
  else {
    logger.info("COMPILATION SUCCESS")
    val executionJob = server.registerJob(gmqlScript, gmqlContext, "")
    logger.info("EXECUTING THE QUERY")
    server.execute(executionJob)
    // Wait
    do {
      logger.info("Waiting for completion")
      Thread.sleep(1000)
    } while(executionJob.getJobStatus != manager.Status.SUCCESS && executionJob.getJobStatus!=manager.Status.EXEC_FAILED)
  }
}
