package it.polimi.genomics.manager.test


import it.polimi.genomics.core.{GDMSUserClass, GMQLSchemaFormat, GMQLScript, ImplementationPlatform}
import it.polimi.genomics.manager
import it.polimi.genomics.manager.{GMQLContext, GMQLExecute, GMQLJob}
import it.polimi.genomics.repository.{Utilities => RepoUtilities}
import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.slf4j.LoggerFactory



object TestOptim {



  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  val logger = LoggerFactory.getLogger(TestOptim.getClass)


  // 0: configuration folder
  def main(args: Array[String]): Unit = {

    var confPath = "/Users/andreagulino/Projects/GMQL-WEB/conf/gmql_conf/"

    // Path to the folder containing the GMQL configuration files
    if(args.length >= 1)
      confPath = args(0)


    // Set the configutation folder path and get the utilities object
    RepoUtilities.confFolder = confPath
    println("repo: "+ RepoUtilities.confFolder)
    val repoUtilities = RepoUtilities()
    val repository = repoUtilities.getRepository()
    val server = GMQLExecute()

    logConfig(repoUtilities)

    val datasetName = "ds_1_1_3 "

    var query = s"DATASET = SELECT() $datasetName;" + s"DATASET1= SELECT() $datasetName; JN=JOIN(DGE(500),DLE(30000),UP) DATASET DATASET1;" +
    s"MATERIALIZE DATASET INTO DATASET_RESULT; MATERIALIZE JN INTO DATASET_RESULT_1;"

    //query = s"DATASET = SELECT() $datasetName;" + s"MATERIALIZE DATASET INTO DATASET_RESULT;"

    val queryName = "queryName"

    val gmqlScript = GMQLScript(query, queryName)
    val gmqlContext = GMQLContext(ImplementationPlatform.SPARK, repository, GMQLSchemaFormat.TAB,
      username = "public", userClass = GDMSUserClass.GUEST)
    val compilationJob = new GMQLJob(gmqlContext, gmqlScript, "public")

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
      } while(executionJob.getJobStatus != manager.Status.SUCCESS)
    }


  }

  def logConfig(repositoryUtilities: RepoUtilities)  = {

    logger.info("Repository information:")
    logger.info("\tRepoDir: " + repositoryUtilities.RepoDir)
    logger.info("\tHDFSRepoDir: " + repositoryUtilities.HDFSRepoDir)
    logger.info("\tMODE: " + repositoryUtilities.MODE)
    logger.info("\tGMQL_REPO_TYPE: " + repositoryUtilities.GMQL_REPO_TYPE)
    logger.info("\tGMQLHOME: " + repositoryUtilities.GMQLHOME)
    logger.info("\tHADOOP_HOME: " + repositoryUtilities.HADOOP_HOME)
    logger.info("\tGMQL_CONF_DIR: " + repositoryUtilities.GMQL_CONF_DIR)
    logger.info("\tGF_ENABLED: " + repositoryUtilities.GF_ENABLED)
  }





}
