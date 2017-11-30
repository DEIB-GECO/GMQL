package it.polimi.genomics.manager

import it.polimi.genomics.core.GDMSUserClass.GDMSUserClass
import it.polimi.genomics.manager.Launchers.GMQLLauncher
import it.polimi.genomics.repository.{GMQLRepository, Utilities => RepoUtils}

abstract class GMQLController(val launcher: GMQLLauncher, configurationPath: String) {

  RepoUtils.confFolder = configurationPath
  val ut: RepoUtils = RepoUtils()
  val repository: GMQLRepository = ut.getRepository()

  /**
    * Compiles the given query.
    *
    * @param username
    * @param query
    * @return a boolean stating if the compilation was successful. Can raise and Exception
    */
  def compile(username: String, query: String): Boolean

  /**
    * Execute the given query.
    *
    * @param username
    * @param userClass
    * @param query
    * @param queryName
    * @param outputFormat
    * @param outputCoordSystem
    * @return
    */
  def executeQuery(username: String, userClass: GDMSUserClass, query: String, queryName: String, outputFormat: String, outputCoordSystem: String): String


  /**
    * Execute the given query.
    *
    * @param username
    * @param userClass
    * @param dag
    * @param queryName
    * @param outputFormat
    * @param outputCoordSystem
    * @return
    */
  def executeDag(username: String, userClass: GDMSUserClass, dag: String, queryName: String, outputFormat: String, outputCoordSystem: String): String


  /**
    * Kills the specified job
    *
    * @param jobId
    */
  def killJob(jobId: String): Unit


  /**
    * Get the status of the specified job
    *
    * @param jobId
    * @return a string specifying the status
    */
  def getStatus(jobId: String): String


  /**
    * Returns the logging for the specified job
    *
    * @param jobId
    * @return a list of strings
    */
  def getLog(jobId: String): List[String]


  /**
    * Returns the list of jobIds for the specified user
    *
    * @param username
    * @return list of jobIds
    */
  def getUserJobs(username: String): List[String]

  /**
    * Returns the message relative to the specified jobId
    *
    * @param jobId
    * @return
    */
  def getMessage(jobId: String): String


  /**
    *
    * Returns the repository object
    *
    * @return
    */
  def getRepository(): GMQLRepository = repository


}
