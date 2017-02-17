package it.polimi.genomics.manager.Launchers

import it.polimi.genomics.manager.{GMQLJob, Status}

/**
  * Abstarct class that defines the basic functions to run GMQL Job {@link GMQLJob}
  *
  * @param job
  */
  abstract class GMQLLauncher(val job:GMQLJob) {

  /**
    *
    * Run GMQL Job and return the handle to this execution
    *
    * @return {@link GMQLLauncher}
    */
    def run(): GMQLLauncher

  /**
    *
    * return the Status of the job
    *
    * @return The {@link GMQLJob} Status {@link Status}
    */
    def getStatus(): Status.Value

  /**
    *
    *  return the {@link GMQLJob} Application name
    *
    * @return String of the application name
    */
    def getAppName (): String

  /**
    *
    * Kill GMQL Job {@link GMQLJob}
    *
    */
    def killJob ()

  }

