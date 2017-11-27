package it.polimi.genomics.manager

import it.polimi.genomics.manager.Launchers.GMQLLauncher

abstract class GMQLController(val launcher: GMQLLauncher) {

  /**
    * Compiles the given query.
    * @param username
    * @param query
    * @return a boolean stating if the compilation was successful. Can raise and Exception
    */
  def compile(username: String, query: String) : Boolean

  /**
    * Execute the given query.
    * @param username
    * @param query
    * @param queryName
    * @param output_type
    * @return the jobID. Can raise Exception
    */
  def execute_query(username: String, query: String, queryName: String, output_type: String): String

  /**
    * Execute the given query.
    * @param username
    * @param dag
    * @param queryName
    * @param output_type
    * @return the jobID. Can raise Exception
    */
  def execute_dag(username: String, dag: String, queryName: String, output_type: String): String


  /**
    * Kills the specified job
    * @param jobId
    */
  def killJob(jobId: String): Unit


  /**
    * Get the status of the specified job
    * @param jobId
    * @return a string specifying the status
    */
  def getStatus(jobId: String): String


  /**
    * Returns the logging for the specified job
    * @param jobId
    * @return a list of strings
    */
  def getLog(jobId: String): List[String]


  /**
    * Returns the list of jobIds for the specified user
    * @param username
    * @return list of jobIds
    */
  def getUserJobs(username: String): List[String]

  /**
    * Returns the message relative to the specified jobId
    * @param jobId
    * @return
    */
  def getMessage(jobId: String) : String

}
