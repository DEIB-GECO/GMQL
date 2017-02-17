package it.polimi.genomics.spark.implementation.MetaOperators.SelectMeta

import it.polimi.genomics.core.DataTypes.MetaType
import it.polimi.genomics.core.{DataTypes, GMQLLoader}
import it.polimi.genomics.spark.implementation.loaders.Loaders._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

/**
 * Created by abdulrahman kaitoua on 05/05/15.
 */
object ReadMD {
  private final val logger = LoggerFactory.getLogger(ReadMD.getClass);
  def apply(paths: List[String], loader: GMQLLoader[Any, Any, Any, Any], sc : SparkContext) : RDD[MetaType] = {
    logger.info("----------------ReadMD executing..")
    def parser(x: (Long, String)) = loader.asInstanceOf[GMQLLoader[(Long, String), Option[DataTypes.GRECORD], (Long, String), Option[DataTypes.MetaType]]].meta_parser(x)
//    val ds = new IRDataSet(path(0),List[(String,PARSING_TYPE)]().asJava)
//    val repo = new LFSRepository()

    val conf = new Configuration();
    val path = new Path(paths.head);
    val fs = FileSystem.get(path.toUri(), conf);

    var files =
//      if (path.size == 1 && repo.DSExists(ds,General_Utilities().USERNAME)) {
//      val username = if(new LFSRepository().DSExistsInPublic(ds)) "public" else General_Utilities().USERNAME
//
//      try {
//        repo.ListDSSamples(path(0),username).asScala.map(d =>
//          if(General_Utilities().MODE.equals(General_Utilities().HDFS))
//            General_Utilities().getHDFSRegionDir(username)+d.meta else d.meta
//        )
//      } catch {
//        case ex: JAXBException => logger.error("DataSet is corrupted"); List[String]()
//        case ex: FileNotFoundException => logger.error("DataSet is not Found"); List[String]()
//      }
//    } else
        paths.flatMap { dirInput =>
          val file = new Path(dirInput)
        if (fs.isDirectory(file))
          fs.listStatus(file,new PathFilter {
            override def accept(path: Path): Boolean = fs.exists(new Path(path.toString + ".meta"))
          }).map(x => x.getPath.toString).toList
//          ).filter(!_.getName.endsWith(".meta")).map(x => x.getPath)
        else List(dirInput)
      }

    val metaPath = files.map(x=>x+".meta").mkString(",")
    sc forPath(metaPath) LoadMetaCombineFiles (parser)
  }
}
