package it.polimi.genomics.spark.implementation.RegionsOperators.SelectRegions

import it.polimi.genomics.core.DataTypes.{GRECORD, MetaType}
import it.polimi.genomics.core.{GMQLLoader, GRecordKey, GValue}
import it.polimi.genomics.spark.implementation.loaders.Loaders._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

/**
  * Created by Luca Nanni on 10/05/2019
  */
object ReadFileRD {

  private final val logger = LoggerFactory.getLogger(ReadFileRD.getClass);

  def apply(filepath: String, loader: GMQLLoader[Any, Any, Any, Any], sc: SparkContext): RDD[GRECORD] = {
    logger.info("----------------ReadFileRD executing..")
    def parser(x: (Long, String)): Option[(GRecordKey, Array[GValue])] =
      loader.asInstanceOf[GMQLLoader[(Long, String), Option[GRECORD], (Long, String), Option[MetaType]]].region_parser(x)
    sc.forPath(List(filepath).mkString(",")).LoadRegionsCombineFiles(parser)
  }
}
