package it.polimi.genomics.manager.Debug

import java.io.File

import it.polimi.genomics.repository.{Utilities=>RepoUtilities}
import org.apache.log4j.{Level, Logger}
import org.slf4j
import org.slf4j.LoggerFactory


import scala.xml.XML

object Main {

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  val logger: slf4j.Logger = LoggerFactory.getLogger(Main.getClass)

  private final val usage: String = "TrainGen Options: \n" +
    "\t"+"-conf"+" : "+"folder containing the TrainGen configuration files;"

  private final val MAIN_FILE = "conf.xml"
  private final val GENERATOR_FILE = "generator.xml"
  private final val EXECUTOR_FILE = "executor.xml"
  private final val GMQL_CONF_DIR = "gmql_conf/"


  class Conf(confDir: String) {

    private val conf_xml = XML.load(confDir+"/"+MAIN_FILE)

    // Parse
    val properties: Map[String, String] = (conf_xml \\ "property").map(n=> (n \\ "name").text -> (n \\ "value").text).toMap[String, String]
    def tempDir : String = properties("temp_dir")
    def genDir: String = properties("gen_dir")
    def outDir: String = properties("out_dir")
    def skipGen: Boolean = properties("skip_gen") == "true"


    def cpuFreq: Float = properties("cpu_freq").toFloat
    def minNumCores: Int =  properties("min_num_cores").toInt
    def maxNumCores: Int = properties("max_num_cores").toInt
    def stepNumCores: Int = properties("step_num_cores").toInt
    def minMemSize: Int =  properties("min_mem_size").toInt
    def maxMemSize: Int = properties("max_mem_size").toInt
    def stepMemSize: Int = properties("step_mem_size").toInt

    def log(): Unit = {
      properties.map(p=>p._1+"->"+p._2).foreach(println)
    }
  }


  def main(args: Array[String]): Unit = {

    var confDir =  "/Users/andreagulino/Desktop/conf/"

    // Read Options
    for (i <- args.indices if i % 2 == 0) {
      if ("-h".equals(args(i)) || "-help".equals(args(i))) {
        println(usage)
        System.exit(0)
      } else if ("-conf".equals(args(i))) {
        confDir = args(i + 1)
        logger.info("-conf: " + confDir)
      } else {
        logger.warn(s"Command option is not found ${args(i)}")
        System.exit(0)
      }
    }

    // Read main configuration file
    val conf = new Conf(confDir)

    val gmqlConfDir = confDir+"/"+GMQL_CONF_DIR

    RepoUtilities.confFolder = gmqlConfDir

    conf.log()

    val cpu_range = Array.range(conf.minNumCores, conf.maxNumCores, conf.stepNumCores)
    val memory_range = Array.range(conf.minMemSize, conf.maxMemSize, conf.stepMemSize)
    val ds_num = AutomatedGenerator.getNumDatasets(confDir+"/"+GENERATOR_FILE)

    val totalExecutions = cpu_range.length * memory_range.length * ds_num

    println("Total number of executions: "+totalExecutions+". Do you want to continue?[y/N]: ")
    val ans = scala.io.StdIn.readLine()
    if(ans.trim!="y")
      System.exit(0)

    if(!conf.skipGen) {
      AutomatedGenerator.go(confDir+"/"+GENERATOR_FILE, conf.tempDir, conf.genDir)
    }


    // Query
    val dss = getListOfSubDirectories(conf.genDir)

    for(cpus <- cpu_range)
      for(mem <- memory_range)
        for(ds <- dss){

          val name = new File(ds) .getName
          logger.info(s"Querying Dataset $ds")
          //val query = s"D1=SELECT() $name; D2=SELECT() $name; D3=JOIN(DLE(0)) D1 D2; MATERIALIZE D3 INTO query_$name;"
          val query = s"D1=SELECT() $name; D2=COVER(2,ANY) D1; MATERIALIZE D2 INTO query_$name;"
          val query_name = s"query_$name"

          // Add execution settings

          Executor.go(confDir = gmqlConfDir, datasets = List(ds),
            query, queryName = query_name, username = "public", conf.outDir, cores = cpus, memory = mem, cpu_freq = conf.cpuFreq)

        }

  }


  private def getListOfSubDirectories(directoryName: String): Array[String] = {
    (new File(directoryName))
      .listFiles
      .filter(_.isDirectory)
      .map(_.getAbsolutePath)
  }

}
