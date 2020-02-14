package it.polimi.genomics.manager.Debug

import java.io.File

import it.polimi.genomics.repository.{Utilities => RepoUtilities}
import org.apache.log4j.{Level, Logger}
import org.slf4j
import org.slf4j.LoggerFactory

import scala.xml.{NodeSeq, XML}
import java.util.Random


object Main {



  def getTriplet(root: NodeSeq, property: String): Array[Int] = {
    Array.range( ( root \\ property \@ "from").toInt, (root \\ property \@ "to").toInt, (root \\ property \@ "step").toInt)
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

    // Parse
    val properties: Map[String, String] = (conf_xml \\ "property").map(n=> (n \\ "name").text -> (n \\ "value").text).toMap[String, String]
    def tempDir : String = properties("temp_dir")
    def genDir: String = properties("gen_dir")
    def outDir: String = properties("out_dir")
    def skipGen: Boolean = properties("skip_gen") == "true"

    def maxExperiments: Int = properties("max_experiments").toInt

    def cpuFreq: Float = properties("cpu_freq").toFloat

    def getCoresRange: Array[Int] = getTriplet(exec_xml \\ "conf" \\ "execution", "num_cores")
    def getMemoryRange: Array[Int] = getTriplet(exec_xml \\ "conf" \\ "execution", "mem_size")
    def getBinRange: Array[Int] = getTriplet(exec_xml \\ "conf" \\"execution", "bin_size")

    def log(): Unit = {
      properties.map(p=>p._1+"->"+p._2).foreach(println)
    }
  }


  def main(args: Array[String]): Unit = {

    var confDir =  "/Users/andreagulino/Desktop/TEX/conf/"

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
    val memory_range = conf.getMemoryRange // actually not used
    val mem = memory_range.head // because is set as an option of JVM in local mode
    val bin_range = conf.getBinRange
    val query = new Query(confDir+"/"+QUERY_FILE)
    val ds_num = AutomatedGenerator.getNumDatasets(confDir+"/"+GENERATOR_FILE, query.isBinary)

    println("CORES: "+cpu_range.length+" ("+cpu_range.mkString(",")+")\n"+
      "MEMORY: "+memory_range.length+" ("+memory_range.mkString(",")+")\n"+
      "DATASETS: "+ds_num+"\n"+
      "BINNING: "+bin_range.length+" ("+bin_range.mkString(",")+")\n")

    val totalExecutions = cpu_range.length * memory_range.length * ds_num * bin_range.length * query.getNumQueries

    println("["+query.operatorName+"]Total number of executions: "+totalExecutions+". Do you want to continue?[y/N]: ")
    val ans = scala.io.StdIn.readLine()
    if(ans.trim!="y")
      System.exit(0)


    // Generate Datasets and Import them into the repository
    if(!conf.skipGen) {
      AutomatedGenerator.generate(confDir+"/"+GENERATOR_FILE, conf.tempDir, conf.genDir, query.isBinary)
    }


    // Get List of datasets
    val dss : List[List[String]] =
      if(!query.isBinary)
        getListOfSubDirectories(conf.genDir+"/unary/").map(d=>List(d)).toList
      else {
        val references = getListOfSubDirectories(conf.genDir+"/binary/reference/")
        val experiments = getListOfSubDirectories(conf.genDir+"/binary/experiment/")

        references.flatMap(r => experiments.map(e => List(r,e))).toList

      }


    case class RunninConfig(bin:Int, query: String, cpus: Int, datasets: List[String], queryName: String)

    var configurations = scala.collection.mutable.ArrayBuffer[RunninConfig]()

    for(bin_size <- bin_range)
        for(cpus <- cpu_range)
          for (ds <- dss) {
            val dss_name = ds.map(f=>new File(f).getName).mkString("_")
            val id = s"query_${dss_name}_bin_${bin_size}_cpus_${cpus}_mem_${mem}g"
            val queries = query.getQueries(ds.map(fp=>new File(fp).getName), id)
            for (q <- queries)
              //q: (queryName, query)
              configurations += RunninConfig(bin_size, q._2, cpus, ds, q._1)
          }



    val rand = new Random(System.currentTimeMillis())

    val max = Math.min(conf.maxExperiments, configurations.length)

    for(i <- 1 to max) {


      val random_index = rand.nextInt(configurations.length)
      val cc = configurations(random_index)

      configurations.remove(random_index)

      println("executing query: "+cc.query)


      Executor.go(confDir = gmqlConfDir, datasets = cc.datasets,
        cc.query, queryName = cc.queryName, username = "public",
        conf.outDir, cores = cc.cpus, memory = mem,
        cpu_freq = conf.cpuFreq,
        bin_size = cc.bin)

    }

  }


  private def getListOfSubDirectories(directoryName: String): Array[String] = {
    (new File(directoryName))
      .listFiles
      .filter(_.isDirectory)
      .map(_.getAbsolutePath)
  }

}
