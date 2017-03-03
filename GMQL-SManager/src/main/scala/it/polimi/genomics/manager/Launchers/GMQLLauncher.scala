package it.polimi.genomics.manager.Launchers

import it.polimi.genomics.manager.{GMQLJob, Status}

/**
  * Abstarct class that defines the basic functions to run GMQL Job [[ GMQLJob]]
  *
  * @param job
  */
  abstract class GMQLLauncher(val job:GMQLJob) {

  /**
    *
    * Run GMQL Job and return the handle to this execution
    *
    * @return [[ GMQLLauncher]]
    */
    def run(): GMQLLauncher

  /**
    *
    * return the Status of the job
    *
    * @return The [[ GMQLJob]] Status [[ Status]]
    */
    def getStatus(): Status.Value

  /**
    *
    *  return the [[ GMQLJob]] Application name
    *
    * @return String of the application name
    */
    def getAppName (): String

  /**
    *
    * Kill GMQL Job [[ GMQLJob]]
    *
    */
    def killJob ()

  }

