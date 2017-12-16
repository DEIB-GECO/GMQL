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

    def parser(x: (Long, String)) =
      loader
        .asInstanceOf[GMQLLoader[(Long, String), Option[DataTypes.GRECORD], (Long, String), Option[DataTypes.MetaType]]]
        .meta_parser(x)

    val conf = new Configuration()
    val path = new Path(paths.head)
    val fs = FileSystem.get(path.toUri(), conf)

    var files =
      paths.flatMap { dirInput =>
        val file = new Path(dirInput)
        if (fs.isDirectory(file))
          fs.listStatus(file,new PathFilter {
            override def accept(path: Path): Boolean = fs.exists(new Path(path.toString + ".meta"))
          }).map(x => x.getPath.toString).toList
        else List(dirInput)
      }

    val metaPath = files.map(x=>x+".meta").mkString(",")
    sc forPath(metaPath) LoadMetaCombineFiles (parser)
  }
}