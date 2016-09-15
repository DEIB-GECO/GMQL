package it.polimi.genomics.spark.implementation.MetaOperators

import it.polimi.genomics.core.DataStructures.{MetaOperator, RegionOperator}
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

/**
 * Created by abdulrahman kaitoua on 01/06/15.
 */
object PurgeMD {
  private final val logger = LoggerFactory.getLogger(this.getClass);

  @throws[SelectFormatException]
  def apply(executor : GMQLSparkExecutor, regionDataset : RegionOperator, inputDataset : MetaOperator, sc : SparkContext) : RDD[MetaType] = {
    logger.info("----------------PurgeMD executing..")

    val input = executor.implement_md(inputDataset, sc)
    val metaIdList = executor.implement_rd(regionDataset, sc).keys.map(x=>x._1).distinct.collect
    input.filter((a : MetaType) => metaIdList.contains(a._1))

  }
}
