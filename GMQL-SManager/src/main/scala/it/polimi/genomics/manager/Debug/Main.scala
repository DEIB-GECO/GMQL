package it.polimi.genomics.manager.Debug

import java.io.File

import it.polimi.genomics.repository.{Utilities => RepoUtilities}
import it.polimi.genomics.manager.{Utilities => ManagerUtilities}


import scala.xml.{NodeSeq, XML}
import java.util.Random

import it.polimi.genomics.repository.FSRepository.FS_Utilities


object Main {



  def getTriplet(root: NodeSeq, property: String): Array[Int] = {

    if( (root \\ property).head.attribute("exact").isDefined )
      ( root \\ property \@ "exact").split(",").map(_.toInt)
    else
      Array.range( ( root \\ property \@ "from").toInt, (root \\ property \@ "to").toInt, (root \\ property \@ "step").toInt)
  }


  //Logger.getLogger("org").setLevel(Level.WARN)
  //Logger.getLogger("akka").setLevel(Level.WARN)

  //val logger: slf4j.Logger = LoggerFactory.getLogger(Main.getClass)

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
        //logger.info("-conf: " + confDir)
      } else {
        //logger.warn(s"Command option is not found ${args(i)}")
        System.exit(0)
      }
    }

    // Read main configuration file
    val conf = new Conf(confDir)

    val gmqlConfDir = confDir+"/"+GMQL_CONF_DIR
    RepoUtilities.confFolder = gmqlConfDir

    conf.log()


    val hadoop_conf = FS_Utilities.gethdfsConfiguration()
    println("###### DEFAULT FS SET TO: "+hadoop_conf.get("fs.defaultFS"))
    println("###### GMQL JAR PATH: "+ManagerUtilities().CLI_JAR_local)

    val cpu_range = conf.getCoresRange
    val memory_range = conf.getMemoryRange // actually not used
    val mem = memory_range.head // because is set as an option of JVM in local mode
    val bin_range = conf.getBinRange
    val query = new Query(confDir+"/"+QUERY_FILE)

    // Generate datasets configurations
    val dsConfigs = AutomatedGenerator.getConfigurations(confDir+"/"+GENERATOR_FILE, query.isBinary, conf.genDir)


    println("CORES: "+cpu_range.length+" ("+cpu_range.mkString(",")+")\n"+
      "MEMORY: "+memory_range.length+" ("+memory_range.mkString(",")+")\n"+
      "DATASETS: "+dsConfigs.length+"\n"+
      "BINNING: "+bin_range.length+" ("+bin_range.mkString(",")+")\n")

    val totalExecutions = cpu_range.length * memory_range.length * dsConfigs.length * bin_range.length * query.getNumQueries

    println("["+query.operatorName+"]Original number of configurations: "+totalExecutions+". At most "+conf.maxExperiments+" configurations will be executed.")



    // Generate all configurations (oncluding different bin size, cpu, datasets and queries)
    case class RunninConfig(bin:Int, query: String, cpus: Int, datasets: List[AutomatedGenerator.DatasetConfig], queryName: String)


    val cfgs = bin_range.flatMap(
      bs => cpu_range.flatMap(
        cpu => dsConfigs.flatMap(
          ds => {

            val dss_name = ds.map(_.name).mkString("_")
            val id = s"query_${dss_name}_bin_${bs}_cpus_${cpu}_mem_${mem}g"
            val queries = query.getQueries( ds.map(_.name), id)

            queries.map(
              q=> {
                RunninConfig(bs, q._2, cpu, ds, q._1)
              }
            )

          }
        )
      )
    ).toList

    var configs = scala.collection.mutable.ArrayBuffer[RunninConfig]()
    cfgs.foreach(e=>{configs+=e})


    // Delete output folders
    AutomatedGenerator.cleanGenFolder(conf.genDir)


    // Execute a limited number of configurations randomly chosen among all the possible configurartions
    val rand = new Random(System.currentTimeMillis())
    val max = Math.min(conf.maxExperiments, configs.length)
    var i = 0

    while(i <= max && configs.nonEmpty) {

      val random_index = rand.nextInt(configs.length)
      val cc = configs(random_index)

      configs.remove(random_index)

      println("generating datasets")

      cc.datasets.foreach( d =>  AutomatedGenerator.generate(d, conf.tempDir))


      println("executing query: "+cc.query)
      println("passing datasets: "+cc.datasets.map( d=> {d.folder_path + d.name}).mkString(","))

      Executor.go(confDir = gmqlConfDir, datasets = cc.datasets.map( d=> {d.folder_path + d.name}),
        cc.query, queryName = cc.queryName, username = "public",
        conf.outDir, cores = cc.cpus, memory = mem,
        cpu_freq = conf.cpuFreq,
        bin_size = cc.bin)

      i=i+1


    }



  }


  private def getListOfSubDirectories(directoryName: String): Array[String] = {
    (new File(directoryName))
      .listFiles
      .filter(_.isDirectory)
      .map(_.getAbsolutePath)
  }

}
