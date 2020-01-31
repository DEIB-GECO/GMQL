package it.polimi.genomics.manager.Debug

import java.io.File

import it.polimi.genomics.repository.{Utilities => RepoUtilities}
import org.apache.log4j.{Level, Logger}
import org.slf4j
import org.slf4j.LoggerFactory

import scala.xml.{Elem, XML}

object Main {

  def getTriplet(xml: Elem, root: String, property: String): Array[Int] = {
    Array.range( (xml \\ "conf" \\ root \\ property \@ "from").toInt, (xml \\ property \@ "to").toInt, (xml \\ property \@ "step").toInt)
  }


  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  val logger: slf4j.Logger = LoggerFactory.getLogger(Main.getClass)

  private final val usage: String = "TrainGen Options: \n" +
    "\t"+"-conf"+" : "+"folder containing the TrainGen configuration files;"

  private final val MAIN_FILE = "conf.xml"
  private final val GENERATOR_FILE = "generator.xml"
  private final val EXECUTOR_FILE = "execution.xml"
  private final val QUERY_FILE = "query.xml"
  private final val GMQL_CONF_DIR = "gmql_conf/"


  class Conf(confDir: String) {

    private val conf_xml = XML.load(confDir+"/"+MAIN_FILE)
    private val exec_xml = XML.load(confDir+"/"+EXECUTOR_FILE)
    private val query_xml = XML.load(confDir+"/"+QUERY_FILE)


    // Parse
    val properties: Map[String, String] = (conf_xml \\ "property").map(n=> (n \\ "name").text -> (n \\ "value").text).toMap[String, String]
    def tempDir : String = properties("temp_dir")
    def genDir: String = properties("gen_dir")
    def outDir: String = properties("out_dir")
    def skipGen: Boolean = properties("skip_gen") == "true"

    def cpuFreq: Float = properties("cpu_freq").toFloat

    def getCoresRange: Array[Int] = getTriplet(exec_xml, "execution", "num_cores")
    def getMemoryRange: Array[Int] = getTriplet(exec_xml, "execution", "mem_size")
    def getBinRange: Array[Int] = getTriplet(exec_xml, "execution", "bin_size")


    def getCoverMinAccRange: Array[Int] = getTriplet(query_xml, "query", "min_acc_cover")



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

    val cpu_range = conf.getCoresRange
    val memory_range = conf.getMemoryRange
    val bin_range = conf.getBinRange
    val min_acc_range = conf.getCoverMinAccRange
    val ds_num = AutomatedGenerator.getNumDatasets(confDir+"/"+GENERATOR_FILE)

    println("CORES: "+cpu_range.length+" ("+cpu_range.mkString(",")+")\n"+
      "MEMORY: "+memory_range.length+" ("+memory_range.mkString(",")+")\n"+
      "DATASETS: "+ds_num+"\n"+
      "BINNING: "+bin_range.length+" ("+bin_range.mkString(",")+")\n"+
      "MIN_ACC: "+min_acc_range.length+" ("+bin_range.mkString(",")+")")

    val totalExecutions = cpu_range.length * memory_range.length * ds_num * bin_range.length*min_acc_range.length

    println("Total number of executions: "+totalExecutions+". Do you want to continue?[y/N]: ")
    val ans = scala.io.StdIn.readLine()
    if(ans.trim!="y")
      System.exit(0)

    if(!conf.skipGen) {
      AutomatedGenerator.go(confDir+"/"+GENERATOR_FILE, conf.tempDir, conf.genDir)
    }


    val mem = memory_range.head
    // Query
    val dss = getListOfSubDirectories(conf.genDir)

    for(bin_size <- bin_range)
      for(min_acc <- min_acc_range)
        for(cpus <- cpu_range) {
          //for(mem <- memory_range)
          for (ds <- dss) {

            val name = new File(ds).getName
            val query_name = s"query_${name}_bin_${bin_size}_minAcc_${min_acc}_cpus_${cpus}_mem_${mem}g"
            logger.info(s"Querying Dataset $ds")
            //val query = s"D1=SELECT() $name; D2=SELECT() $name; D3=JOIN(DLE(0)) D1 D2; MATERIALIZE D3 INTO query_$name;"
            val query = s"D1=SELECT() $name; D2=COVER($min_acc,ANY) D1; MATERIALIZE D2 INTO query_$query_name;"
            println(query)


            // Add execution settings

            Executor.go(confDir = gmqlConfDir, datasets = List(ds),
              query, queryName = query_name, username = "public",
              conf.outDir, cores = cpus, memory = mem,
              cpu_freq = conf.cpuFreq,
              bin_size=bin_size)

          }
        }

  }


  private def getListOfSubDirectories(directoryName: String): Array[String] = {
    (new File(directoryName))
      .listFiles
      .filter(_.isDirectory)
      .map(_.getAbsolutePath)
  }

}
