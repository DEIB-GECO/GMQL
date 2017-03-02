package it.polimi.genomics.scidbapi.script

import java.io.{File, PrintWriter}
import sys.process._

import it.polimi.genomics.scidbapi.SciAbstractArray

/**
  * This class provides an environment to build a SciDB script and
  * run it or exporting the resulting query
  */
class SciScript
{
  var statements : List[SciStatement] = List()
  var queues : List[List[SciStatement]] = List()

  var timer : Long = System.nanoTime()

  // ------------------------------------------------------------
  // -- MANAGEMENT ----------------------------------------------

  /**
    * Returns a temporary name for the script
    *
    * @return a temporary name
    */
  def getTmpName() : String =
    "TMP_"+ timer +"_"+ (statements.size+1)

  /**
    * Adds a new statement into the script list
    *
    * @param statement inserted statement
    */
  def addStatement(statement:SciStatement) =
  {
    statements = statements ::: List(statement)
  }

  def freeStatements() =
  {
    this.statements = List()
  }

  /**
    * Adds a new statment into the temporary queue
    *
    * @param statement queued statment
    */
  def addQueueStatement(statement:SciStatement) =
  {
    if(queues.nonEmpty)
      queues = queues.init ::: List(queues.last ::: List(statement))
    else
      queues = List(List(statement))
  }

  def openQueue() =
  {
    queues = queues ::: List(List())
  }

  def closeQueue() =
  {
    if(queues.size > 1){
      val closed = queues.last
      queues = queues.init
      queues = queues.init ::: List(queues.last ::: closed)
    }
  }

  /**
    * Appends all the statements waiting into the queue
    * to the current execution statements list
    */
  def flushQueue() =
  {
    if(queues.nonEmpty) {
      statements = statements ::: queues.last
      queues = queues.init ::: List(List())
    }
  }

  // ------------------------------------------------------------
  // -- EXPORT --------------------------------------------------

  /**
    * Returns the final query to run the script inside a SciDB
    * environment
    *
    * @return the AFL query
    */
  def query() : String =
  {
    "set lang afl;\n\n" + statements.map( _ match {
      case stmt:SciAbstractArray => "\n" + stmt.getStatementQuery() + ";\n"
      case stmt:SciComment => stmt.getStatementQuery()
      case stmt => stmt.getStatementQuery() + ";"
    }).mkString("\n")
  }

  /**
    * Exports the script query into a file into a desired folder
    * and using the specified file name
    *
    * @param path output directory
    * @param filename export file name
    */
  def export(path:String, filename:String) =
  {
    val writer = new PrintWriter(new File(path+filename))
    writer.write(query())
    writer.close()
  }

  // ------------------------------------------------------------
  // -- EXECUTION -----------------------------------------------

  /**
    * Runs the query using the local or a remote SciDB server
    *
    * @param server_ip remote server IP
    * @param server_user remote server ssh user
    * @param server_pass remote server ssh password
    * @param server_dir remote server directory to load the query
    */
  def run(server_ip:String = null,
          server_user:String = null,
          server_pass:String = null,
          server_dir:String = null,
          thread:String = "main") : Unit =
  {
    if(server_ip != "local") {

      // query transmission -------------------------
      val filename = "query_" + "client-X" + thread + "_" + System.currentTimeMillis + ".afl"
      export("../output/", filename)
      (/*"sshpass -p "+ server_pass +" "+*/ "scp /home/scidb/output/" + filename + " " + server_user + "@" + server_ip + ":" + server_dir).!
      ("rm ../output/" + filename).!

      // iquery cmd preparation ---------------------
      var iqcmd = "/opt/scidb/15.12/bin/iquery" +
        " -af " + server_dir + "/" + filename + ""

      // cmd preparation ----------------------------
      var cmd = //"sshpass -p "+ server_pass +" "+
        "ssh " + server_user + "@" + server_ip +
          " " + iqcmd.replace("\"", "\\\"") + ""

      // command execution --------------------------
      cmd.!

    }else{

      // query transmission -------------------------
      val filename = "query_" + "client-X" + thread + "_" + System.currentTimeMillis + ".afl"
      export(server_dir+"/", filename)

      // iquery cmd preparation ---------------------
      var iqcmd = "iquery" +
        " -af " + server_dir + "/" + filename + ""

      // command execution --------------------------
      iqcmd.!

    }
  }

  /**
    * Runs the query using a local or a remote SciDB server
    * and returns the obtained result as CSV string
    *
    * @param server_ip remote server IP
    * @param server_user remote server ssh user
    * @param server_pass remote server ssh password
    * @param server_dir remote server directory to load the query
    * @return
    */
  def ocsv(server_ip:String = null,
           server_user:String = null,
           server_pass:String = null,
           server_dir:String = null) : String =
  {
    if(server_ip != "local") {

      // query transmission -------------------------
      val filename = "query_"+ "client-X001" +"_"+ System.currentTimeMillis +".afl"
      export("../output/",filename)

      ("scp /home/scidb/output/"+ filename +" "+ server_user +"@"+ server_ip +":"+ server_dir).!
      ("rm ../output/"+ filename).!

      // iquery cmd preparation ---------------------
      var iqcmd = "/opt/scidb/15.12/bin/iquery" +
        " -ocsv -af "+ server_dir +"/"+ filename +""

      // cmd preparation ----------------------------
      var cmd = //"sshpass -p "+ server_pass +
        "ssh "+ server_user +"@"+ server_ip +
        " "+ iqcmd.replace("\"", "\\\"") + ""

      // command execution --------------------------
      cmd.!!

    }else{

      // query transmission -------------------------
      val filename = "query_"+ "client-X001" +"_"+ System.currentTimeMillis +".afl"
      export(server_dir+"/", filename)

      // iquery cmd preparation ---------------------
      var iqcmd = "iquery" +
        " -ocsv -af "+ server_dir +"/"+ filename +""

      // command execution --------------------------
      iqcmd.!!

    }

  }

  // ------------------------------------------------------------
  // -- OVERRIDE ------------------------------------------------

  override def toString() : String =
  {
    statements.toString()
  }

}
