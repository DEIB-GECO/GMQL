package it.polimi.genomics.core

/**
  * Created by abdulrahman on 23/01/2017.
  * Contains both the script path and String of the Script
  */
/**
  *
  * @param script [[ String]] of the GMQL script
  * @param scriptPath [[ String]] of the path to the script on local file system
  */
case class GMQLScript (script:String, scriptPath:String, var dag: String = "", var dagPath: String = "")
