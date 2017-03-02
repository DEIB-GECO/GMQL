package it.polimi.genomics.scidb.repository

import java.io.File

import it.polimi.genomics.scidb.GmqlSciConfig
import it.polimi.genomics.scidb.exception.IOErrorGmqlSciException
import org.slf4j.LoggerFactory

import scala.io.Source
import scala.sys.process._

object GmqlSciFileSystem
{
  /**
    * Executes a UNIX command on the default server using SSH
    *
    * @param command UNIX command to be executed
    * @return command result string
    */
  def remote(command:String) : String =
  {
    var cmd = //"sshpass -p "+ GmqlSciConfig.scidb_server_password +" "+
      "ssh "+ GmqlSciConfig.scidb_server_username +"@"+ GmqlSciConfig.scidb_server_ip +
      " "+ command
    cmd.!!
  }

  // ------------------------------------------------------------
  // -- COMMANDS ------------------------------------------------

  /**
    * Returns the list of the
    *
    * @param file file load
    * @return the source to read the file
    */
  def cat(file:String) : Source =
  {
    if( GmqlSciConfig.scidb_server_remote ){

      // ------------------------------------------------
      // Remote execution -------------------------------

      Source.fromString( remote("cat "+file) )

    }else{

      // ------------------------------------------------
      // Local execution --------------------------------

      val obj = new File(file)

      if( !obj.exists() )
        throw new IOErrorGmqlSciException("Invalid file, '"+ file +"' not found")

      Source.fromFile(obj)

    }
  }

  /**
    * Returns the list of the file contained in the directory
    *
    * @param path directory path
    * @return list of contained files
    */
  def ls(path:String) : List[File] =
  {
    if( GmqlSciConfig.scidb_server_remote ){

      // ------------------------------------------------
      // Remote execution -------------------------------

      remote("ls "+ path)
        .split("\n")
        .map(item => new File(path+"/"+item))
        .toList

    }else{

      // ------------------------------------------------
      // Local execution --------------------------------

      val directory = new File(path)

      if( !directory.exists() || !directory.isDirectory )
        throw new IOErrorGmqlSciException("Invalid path, '"+ path +"' not found")

      directory.listFiles.toList

    }
  }

  /**
    * Creates the folder at the required path, even if
    * recursive creation is required for some parents
    *
    * @param path
    */
  def mkdir(path:String) : Unit =
  {
    if( GmqlSciConfig.scidb_server_remote ){

      // ------------------------------------------------
      // Remote execution -------------------------------

      remote("mkdir -p "+path)

    }else{

      // ------------------------------------------------
      // Local execution --------------------------------

      (new File(path)).mkdirs()

    }
  }

  /**
    * Removes a file
    *
    * @param file file to be removed
    */
  def rm(file:String) : Unit =
  {
    if( GmqlSciConfig.scidb_server_remote ){

      // ------------------------------------------------
      // Remote execution -------------------------------

      remote("rm "+ file)

    }else{

      // ------------------------------------------------
      // Local execution --------------------------------

      val obj = new File(file)

      if (!obj.exists())
        throw new IOErrorGmqlSciException("Invalid file, '" + file + "' not found")

      obj.delete()

    }
  }

  def rmR(file:String) : Unit =
  {
    if( GmqlSciConfig.scidb_server_remote ){

      // ------------------------------------------------
      // Remote execution -------------------------------

      remote("rm -R "+ file)

    }else{

      // ------------------------------------------------
      // Local execution --------------------------------

      val obj = new File(file)

      if (!obj.exists())
        throw new IOErrorGmqlSciException("Invalid file, '" + file + "' not found")

      obj.delete()

    }
  }
}
