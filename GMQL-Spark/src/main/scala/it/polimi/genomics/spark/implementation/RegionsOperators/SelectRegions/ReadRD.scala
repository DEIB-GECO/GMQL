package it.polimi.genomics.spark.implementation.RegionsOperators

import java.io.FileNotFoundException
import java.nio.file.Paths
import javax.xml.bind.JAXBException

import it.polimi.genomics.core.DataTypes.{GRECORD, MetaType}
import it.polimi.genomics.core.GMQLLoader
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import it.polimi.genomics.spark.implementation.loaders.Loaders._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.slf4j.LoggerFactory

/**
 * Created by abdulrahman Kaitoua on 25/05/15.
 */
object ReadRD {

  private final val logger = LoggerFactory.getLogger(SelectRD.getClass);
  def apply(paths: List[String], loader : GMQLLoader[Any,Any,Any,Any], sc : SparkContext) : RDD[GRECORD] = {
    def parser(x: (Long, String)) = loader.asInstanceOf[GMQLLoader[(Long, String), Option[GRECORD], (Long, String), Option[MetaType]]].region_parser(x)
//    val ds = new IRDataSet(path(0),List[(String,PARSING_TYPE)]().asJava)
//    val repo = new LFSRepository()

    val conf = new Configuration();
    val path = new org.apache.hadoop.fs.Path(paths.head);
    val fs = FileSystem.get(path.toUri(), conf);

    var files =
//      if (paths.size == 1 && repo.DSExists(ds,General_Utilities().USERNAME)) {
//        val username = if(repo.DSExistsInPublic(ds)) "public" else General_Utilities().USERNAME
//        try {
//          import scala.collection.JavaConverters._
//          repo.ListDSSamples(paths(0),General_Utilities().USERNAME).asScala.map(d =>
//            if (General_Utilities().MODE.equals(General_Utilities().HDFS)) {
//              val hdfs = FSR_Utilities.gethdfsConfiguration().get("fs.defaultFS")
//              hdfs + General_Utilities().HDFSRepoDir +username+ "/regions/" + d.name
//            }
//            else { d.name}
//          )
//        } catch {
//          case ex: JAXBException => logger.error("DataSet is corrupted"); List[String]()
//          case ex: FileNotFoundException => logger.error("DataSet is not Found"); List[String]()
//        }
//      }else {
        paths.flatMap { dirInput =>
          val file = new Path(dirInput)
          if (fs.isDirectory(file))
            fs.listStatus(file, new PathFilter {
              override def accept(path: Path): Boolean = fs.exists(new Path(path.toString + ".meta"))
            }).map(x => x.getPath.toString).toList;
//            new java.io.File(dirInput).listFiles.filter(!_.getName.endsWith(".meta")).map(x => x.getPath)
          else List(dirInput)
        }
//      }
  sc.forPath(files.mkString(",")).LoadRegionsCombineFiles(parser)

  }
}
