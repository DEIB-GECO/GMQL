package it.polimi.genomics.manager.Launchers

/**
  * @author ABDULRAHMAN KAITOUA
  */

import java.io.{ByteArrayOutputStream, IOException, ObjectOutputStream}
import java.util

import com.sun.jersey.core.util.Base64
import it.polimi.genomics.compiler.Operator
import it.polimi.genomics.core.DataStructures.IRDataSet
import it.polimi.genomics.core.ParsingType.PARSING_TYPE
import it.polimi.genomics.manager.{GMQLJob, Utilities}
import org.apache.spark.launcher.{SparkAppHandle, SparkLauncher}

import scala.collection.JavaConverters._
import scala.util.Random
import it.polimi.genomics.repository.{Utilities => General_Utilities}
import it.polimi.genomics.repository.FSRepository.{LFSRepository, FS_Utilities => FSR_Utilities}

/**
  *  Set the configurations for spark launcher to lanch GMQL CLI with arguments
  *
  */
class GMQLSparkSubmit(job:GMQLJob) {

  val SPARK_HOME = Utilities().SPARK_HOME
  val HADOOP_CONF_DIR = General_Utilities().HADOOP_CONF_DIR
  val YARN_CONF_DIR =  General_Utilities().HADOOP_CONF_DIR
  val GMQL_HOME = General_Utilities().GMQLHOME
  val SPARK_UI_PORT = Utilities().SPARK_UI_PORT


  final val GMQLjar:String = Utilities().CLI_JAR_local()
  final val MASTER_CLASS = Utilities().CLI_CLASS
  final val APPID = "GMQL_" + Random.nextInt() + "_" + job.jobId

  //TODO: Make the configuration of the resource Dynamically set, based on the job estimated complexity.
  final val DRIVER_MEM = "10g"
  final val EXECUTOR_MEM = "4g"
  final val NUM_EXECUTORS = "15"
  final val CORES = "30"
  final val DEFAULT_PARALLELISM = "200"

  /**
    * Run GMQL Spark Job using Spark Launcher (client of Spark launcher server)
    * @return
    */
  def runSparkJob(): SparkAppHandle = {
//    println("SparkHome: "+SPARK_HOME)
//    println("HADOOP CONF: "+HADOOP_CONF_DIR)
//    println("YARN CONF: "+YARN_CONF_DIR)
//    println("GMQL HOME: "+GMQL_HOME)
//    println("GMQLJAR: "+GMQLjar)
//    println("MASTER CLASS : "+MASTER_CLASS)
//    println("AppID: "+APPID)
//    println("user: "+job.username)
//    println ("script: "+ job.script.script)
//    println("DS in Dir: "+job.inputDataSets.map(x => x._1+":::"+x._2+"/").mkString(","))
//    println("ds in to schema: "+job.inputDataSets.map(x => x._2+":::"+getSchema(job,x._1)).mkString(","))
//    println("JobID: "+ job.jobId)
//    println("out format: "+ job.gMQLContext.outputFormat.toString)
//    println("log: " +General_Utilities().getLogDir(job.username))


    val env = Map(
      "HADOOP_CONF_DIR" -> HADOOP_CONF_DIR,
      "YARN_CONF_DIR" -> YARN_CONF_DIR
    )

    val outDir = job.outputVariablesList.map{x=>
      val dir = job.renameOutputDirs(x)
      x.substring(job.generateResultName().length+1)+":::"+dir }.mkString(",")

//    println(outDir)

    var d =  new SparkLauncher(env.asJava)
      .setSparkHome(SPARK_HOME)
      .setAppResource(GMQLjar)
      .setMainClass(MASTER_CLASS)
      .setConf("spark.ui.port", SPARK_UI_PORT.toString)
      .setConf("spark.driver.extraClassPath", Utilities().lib_dir_local + "/libs/*")
      //.setConf("spark.executor.extraClassPath", Utilities().lib_dir_local + "/*")
      .addAppArgs("-username", job.username,
        //"-script", job.script.script/*serializeDAG(job.operators)*/,
        //"-scriptpath", job.script.scriptPath,
        //"-inputDirs",job.inputDataSets.map{x =>x._1+":::"+x._2+"/"}.mkString(","),
        //TODO: Check how to get the schema path from the repository manager.
//        "-schemata",job.inputDataSets.map(x => x._2+":::"+getSchema(job,x._1)).mkString(","),
        "-jobid", job.jobId,
        "-outputFormat",job.gMQLContext.outputFormat.toString,
        "-outputCoordinateSystem", job.gMQLContext.outputCoordinateSystem.toString,
        //"-outputDirs", outDir,
        "-logDir",General_Utilities().getLogDir(job.username))
      .setConf("spark.app.id", APPID)
    if(job.script.script != null && job.script.script != "") {
      d = d.addAppArgs("-script", job.script.script)
    }
    if(job.script.scriptPath != null && job.script.scriptPath != "") {
      d = d.addAppArgs("-scriptpath", job.script.scriptPath)
    }
    if(job.inputDataSets.nonEmpty) {
      d = d.addAppArgs("-inputDirs",job.inputDataSets.map{x =>x._1+":::"+x._2+"/"}.mkString(","))
      d = d.addAppArgs("-schemata",job.inputDataSets.map(x => x._2+":::"+getSchema(job,x._1)).mkString(","))
    }
    if(outDir.nonEmpty){
      d = d.addAppArgs("-outputDirs", outDir)
    }

    if(job.script.dag != null && job.script.dag != "") {
      d = d.addAppArgs("-dag", job.script.dag)
    }
    if(job.script.dagPath != null && job.script.dagPath != "") {
      d = d.addAppArgs("-dagpath", job.script.dagPath)
    }

    //d=d.setConf("spark.executor.extraJavaOptions", "-Dlog4j.configuration=file:/Users/canakoglu/GMQL-sources/temp/GMQL/GMQL-Core/src/main/resources/logback.xml")







    // Assign maximum number of executors according to the user category
    if( Utilities().USER_EXECUTORS.contains(job.gMQLContext.userClass) ) {
      d = d.setConf("spark.cores.max", Utilities().USER_EXECUTORS(job.gMQLContext.userClass).toString)
    }

    val b = d.setVerbose(true).startApplication()

    b
  }

  /**
    * reading the data set schema to be sent along with the 
    * @param job
    * @param DS
    * @return
    */
  def getSchema(job:GMQLJob,DS:String):String = {
    import scala.io.Source
    import scala.collection.JavaConverters._

    val repository = new LFSRepository()
    val user = if(repository.DSExistsInPublic(DS))"public" else job.username
    Source.fromFile(General_Utilities().getSchemaDir(user)+DS+".schema").getLines().mkString("")
  }
}