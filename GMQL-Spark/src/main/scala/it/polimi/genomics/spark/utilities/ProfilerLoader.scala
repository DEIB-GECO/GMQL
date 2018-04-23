package it.polimi.genomics.spark.utilities

import com.google.common.hash.Hashing
import it.polimi.genomics.core.DataTypes.{GRECORD, MetaType}
import it.polimi.genomics.core.{DataTypes, GMQLLoader}
import it.polimi.genomics.profiling.Profilers.Profiler
import it.polimi.genomics.profiling.Profiles.GMQLDatasetProfile
import it.polimi.genomics.spark.implementation.loaders.Loaders.Context
import it.polimi.genomics.spark.implementation.loaders.{BasicParser, CustomParser, Loaders}
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.conf.Configuration
import org.slf4j.LoggerFactory

/**
  * Created by andreagulino on 30/09/17.
  */
object ProfilerLoader {

  private final val logger = LoggerFactory.getLogger(this.getClass)

  def profile(datasetpath: String, conf: Configuration) : GMQLDatasetProfile = {

    FSConfig.setConf(conf)
    val fs: FileSystem = FileSystem.get(conf)

    val sc = Spark.sc
    val path   = new Path(datasetpath)

    // Select region files
    val selectedURIs: List[String] =  fs.listStatus(path,
      new PathFilter {
        override def accept(path: Path): Boolean = {
          fs.exists(new Path(path.toString+".meta"))
        }
      }).map(x=>x.getPath.toString).toList


    // Map sample names to sample ids => Map[id,name]
    val samples  =
    selectedURIs.map(
      x => { val name  = new Path(x) getName
        val next =
          if(name.lastIndexOf(".") == -1) name
          else name.substring(0, name.lastIndexOf("."))
        (getSampleID(name), name) }
    ).toMap[Long, String]

    val schemaFile = datasetpath+"/schema.xml"
    val parser = (new CustomParser).setSchema(schemaFile)

    // Parser functions
    def region_parser(x: (Long, String)) = {
      parser.asInstanceOf[GMQLLoader[(Long, String), Option[GRECORD], (Long, String), Option[MetaType]]].region_parser(x)
    }

    def meta_parser(x: (Long, String)) = BasicParser.asInstanceOf[GMQLLoader[(Long, String), Option[DataTypes.GRECORD], (Long, String), Option[DataTypes.MetaType]]].meta_parser(x)
    
    // Get RDDs for regions and meta
    val regions = sc forPath selectedURIs.mkString(",") LoadRegionsCombineFiles(region_parser, false)
    val meta    = sc forPath(selectedURIs.map(x=>x+".meta").mkString(",")) LoadMetaCombineFiles (meta_parser, false)

    val startTime = System.currentTimeMillis()

    // Profile the dataset
    val dsprofile: GMQLDatasetProfile = Profiler.profile(regions, meta, sc, Some(samples))

    val elapsedTime = (System.currentTimeMillis() - startTime)/1000

    // Add profiling time
    logger.info("Profiling completed in "+elapsedTime+" seconds.")

    val xml = Profiler.profileToOptXML(dsprofile)

    dsprofile

  }

  /**
    * Get the sample ID provided its file name
    * @param fileName
    * @return
    */
  private def getSampleID(fileName: String) : Long =
  {
    Hashing.md5().newHasher().putString(fileName.replaceAll("/",""),java.nio.charset.StandardCharsets.UTF_8).hash().asLong()
  }

  /**
    * Set [[SparkContext]] and the input directory path as a [[String]]
    *
    * @param sc  [[SparkContext]] to read the input files.
    * @param path input directory path as a [[String]]
    * @return
    */
  def forPath(sc: SparkContext, path: String) = {
    new Context(sc, path)
  }

  implicit class SparkContextFunctions(val self: SparkContext) extends AnyVal {

    def forPath(path: String): Loaders.Context = Loaders.forPath(self, path)
  }



  def main (args: Array[String] ): Unit = {


    var datasetPath = "/Users/andreagulino/Desktop/files/"

    if(args.length>0) datasetPath = args(0)

    val dsprofile = profile(datasetPath, new Configuration())

    val conf = new Configuration()
    val path = new org.apache.hadoop.fs.Path(datasetPath)
    val fs = FileSystem.get(path.toUri(), conf)

    try {
      val output = fs.create(new Path(datasetPath + "profile.xml"))
      val output_web = fs.create(new Path(datasetPath  + "web_profile.xml"))

      val os = new java.io.BufferedOutputStream(output)
      val os_web = new java.io.BufferedOutputStream(output_web)

      os.write(Profiler.profileToOptXML(dsprofile).toString().getBytes("UTF-8"))
      os_web.write(Profiler.profileToWebXML(dsprofile).toString().getBytes("UTF-8"))

      os.close()
      os_web.close()
    } catch {
      case e: Throwable =>
        logger.error(e.getMessage)
        e.printStackTrace()

    }

  }

}

object Spark {
  val conf = new SparkConf()
    .setAppName("GMQL V2 Spark")
    .setMaster("local[*]")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryoserializer.buffer", "64")
    .set("spark.driver.allowMultipleContexts","true")
    .set("spark.sql.tungsten.enabled", "true")

  var sc: SparkContext =  new SparkContext(conf)
}
