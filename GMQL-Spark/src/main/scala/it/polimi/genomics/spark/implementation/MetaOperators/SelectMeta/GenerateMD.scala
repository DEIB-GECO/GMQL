package it.polimi.genomics.spark.implementation.MetaOperators.SelectMeta

import it.polimi.genomics.core.DataStructures.RegionOperator
import it.polimi.genomics.core.DataTypes.MetaType
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

/**
  * Created by Luca Nanni on 10/05/19
  */
object GenerateMD {
  private final val logger = LoggerFactory.getLogger(GenerateMD.getClass)
  def apply(executor : GMQLSparkExecutor, regionDataset: RegionOperator, sc : SparkContext) : RDD[MetaType] = {
    logger.info("----------------GenerateMD executing..")
    val input = executor.implement_rd(regionDataset, sc)
    input.map{
      case (key, _) => key.id
    }.distinct().map{ id => (id, ("sampleID", id.toString)) }
  }
}
