package it.polimi.genomics.scidb

/**
  * The object provides some global values used to configurate
  * and connect the SciDB server
  */
object GmqlSciConfig
{

  /**
    * Provides some configuration for the developing mode
    */
  val scidb_server_debug : Boolean = false
  val scidb_server_on : Boolean = true
  val scidb_server_verbouse : Boolean = false

  val scidb_server_iquery : String = "/opt/scidb/15.12/bin/iquery"  // /opt/scidb/15.7_local/bin/iquery

  /**
    * Specifies if the server is remote or local
    */
  val scidb_server_remote : Boolean = true

  /**
    * Provides the SciDB server position, at this address should be
    * available the coordinator with iquery
    */
  //val scidb_server_ip : String = "192.168.211.147"
  val scidb_server_ip : String = if(scidb_server_remote) "54.175.158.207" else "local"

  /**
    * Provides the SciDB server access port, open to to connect
    * using the SSH protocol
    */
  val scidb_server_ssh_port : Int = 22

  /**
    * Scidb user allowed to run iquery commands
    */
  val scidb_server_username : String = "scidb"

  /**
    * Password for the scidb user
    */
  //val scidb_server_password : String = "scidb"
  val scidb_server_password : String = "paradigm4"

  /**
    * Directory where queries will be persisted in order to run the iquery
    * command, and then not removed to have the history of executed queries
    */
  val scidb_server_runtime_dir : String = "/home/scidb/runtime"

  /**
    * Directory where will be loaded all the dataset coming from an importation
    */
  val scidb_server_import_dir : String = "/home/scidb/dataset/deploy"

  /**
    * Directory where will be loaded all the dataset coming from an exportation
    */
  val scidb_server_export_dir : String = "/home/scidb/dataset/export"

  // ------------------------------------------------------------
  // ------------------------------------------------------------

  val select_bypass = true
  val chuck_mode = true

  val binning = 1;

}
