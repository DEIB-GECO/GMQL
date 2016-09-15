package it.polimi.genomics.manager.Launchers

/**
  * @AUTHOR ABDULRAHMAN KAITOUA
  */

import java.io.{IOException, ObjectOutputStream, ByteArrayOutputStream}

import com.sun.jersey.core.util.Base64
import it.polimi.genomics.compiler.Operator
import it.polimi.genomics.manager.GMQLJob
import org.apache.spark.launcher.{SparkAppHandle, SparkLauncher}

import scala.collection.JavaConverters._
import scala.util.Random

/**
  *  Set the configurations for spark launcher to lanch GMQL CLI with arguments
  *
  * @param sparkHome
  * @param hadoopConfDir
  * @param yarnConfDir
  * @param GMQLHOME
  * @param someCustomSetting
  * @param scriptPath
  * @param jobid
  * @param username
  */
class GMQLSparkSubmit(job:GMQLJob) {

  val SPARK_HOME = System.getenv("SPARK_HOME")
  val HADOOP_CONF_DIR = System.getenv("HADOOP_CONF_DIR")
  val YARN_CONF_DIR = System.getenv("YARN_CONF_DIR")
  val GMQL_HOME = System.getenv("GMQL_HOME")

  final val GMQLjar = GMQL_HOME + "/utils/lib/GMQL-Cli-2.0-jar-with-dependencies.jar"
  final val MASTER_CLASS = "it.polimi.genomics.cli.GMQLExecuteCommand"
  final val APPID = "GMQL_" + Random.nextInt() + "_" + job.jobId
  final val DRIVER_MEM = "10g"
  final val EXECUTOR_MEM = "4g"
  final val NUM_EXECUTORS = "15"
  final val CORES = "30"
  final val DEFAULT_PARALLELISM = "200"

  def runSparkJob(): SparkAppHandle = {
    val env = Map(
      "HADOOP_CONF_DIR" -> HADOOP_CONF_DIR,
      "YARN_CONF_DIR" -> YARN_CONF_DIR
    )

    new SparkLauncher(env.asJava)
      .setSparkHome(SPARK_HOME)
      .setAppResource(GMQLjar)
      .setMainClass(MASTER_CLASS)
      .addAppArgs("-username", job.username,
        "-script", job.script/*serializeDAG(job.operators)*/,
        "-scriptpath", job.scriptPath,
        "-inputs",job.inputDataSets.map(x => x._1+":::"+x._2+"/").mkString(","),
        //TODO: Check how to get the schema path from the repository manager.
        "-schema",job.inputDataSets.map(x => x._2+":::"+getSchema(job,x._1)).mkString(","),
        "-jobid", job.jobId,
        "-outputFormat",job.outputFormat)
//      .setMaster("yarn-client")
//      .setMaster("local[*]")
      .setConf("spark.app.id", APPID)
//      .setConf("spark.driver.memory", DRIVER_MEM)
//      .setConf("spark.akka.frameSize", "200")
//      .setConf("spark.executor.memory", EXECUTOR_MEM)
//      .setConf("spark.executor.instances", NUM_EXECUTORS)
//      .setConf("spark.executor.cores", CORES)
//      .setConf("spark.default.parallelism", DEFAULT_PARALLELISM)
//      .setConf("spark.driver.allowMultipleContexts", "true")
//      .setConf("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .setConf("spark.kryoserializer.buffer", "64")
//      .setConf("spark.rdd.compress","true")
//      .setConf("spark.akka.threads","8")
//        .setConf("spark.yarn.am.memory","4g") // instead of driver.mem when yarn client mode
//        .setConf("spark.yarn.am.memoryOverhead","600") // instead of spark.yarn.driver.memoryOverhead when client mode
//        .setConf("spark.yarn.executor.memoryOverhead","600")
      .setVerbose(true)
      .startApplication()
  }

  def getSchema(job:GMQLJob,DS:String):String = {
    import scala.io.Source
    import it.polimi.genomics.repository.util.Utilities;
    val user = if(Utilities.getInstance().checkDSNameinPublic(DS))"public" else job.username
    Source.fromFile(Utilities.getInstance().RepoDir+user+"/schema/"+DS+".schema").getLines().mkString("")
  }
  def serializeDAG(dag: List[Operator]): String = {
    try {
      val mylist =  new java.util.ArrayList[Operator]
      for(i <- dag) mylist.add(i)

      val byteArrayOutputStream = new ByteArrayOutputStream();
      val objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
      objectOutputStream.writeObject(mylist);
      objectOutputStream.close();
      new String(Base64.encode(byteArrayOutputStream.toByteArray()));

    } catch {
      case io: IOException => io.printStackTrace(); "none"
    }
  }
}