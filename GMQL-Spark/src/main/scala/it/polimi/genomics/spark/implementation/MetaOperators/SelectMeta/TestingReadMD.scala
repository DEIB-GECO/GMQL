package it.polimi.genomics.spark.implementation.MetaOperators.SelectMeta

import it.polimi.genomics.core.DataTypes.MetaType
import it.polimi.genomics.core.{DataTypes, GMQLLoader}
import it.polimi.genomics.spark.implementation.loaders.Loaders._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

/**
 * Created by pietro on 01/10/15.
 */
object TestingReadMD {
  private final val logger = LoggerFactory.getLogger(TestingReadMD.getClass)
  def apply(path: List[String], loader: GMQLLoader[Any, Any, Any, Any], sc : SparkContext) : RDD[MetaType] = {
    logger.info("----------------TestingReadMD executing..")

    def parser(x: (Long, String)) = loader.asInstanceOf[GMQLLoader[(Long, String), Option[DataTypes.GRECORD], (Long, String), Option[DataTypes.MetaType]]].meta_parser(x)
    val files = path.flatMap { dirInput =>
        if (new java.io.File(dirInput).isDirectory)
          new java.io.File(dirInput).listFiles.filter(x => !(x.getName.endsWith(".meta") || x.isDirectory) ).map(x => x.getPath)
        else List(dirInput)
      }

    val metaPath = files.map(x=>x+".meta").mkString(",")
    sc forPath(metaPath) LoadMetaCombineFiles (parser)
  }
}