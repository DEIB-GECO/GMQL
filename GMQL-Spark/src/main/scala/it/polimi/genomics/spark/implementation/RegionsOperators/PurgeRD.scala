package it.polimi.genomics.spark.implementation.RegionsOperators

import it.polimi.genomics.core.DataStructures.{MetaOperator, RegionOperator}
import it.polimi.genomics.core.DataTypes.GRECORD
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

/**
  * Created by abdulrahman kaitoua on 01/06/15.
  */
object PurgeRD {
  private final val logger = LoggerFactory.getLogger(this.getClass);

  @throws[SelectFormatException]
  def apply(executor: GMQLSparkExecutor, metaDataset: MetaOperator, inputDataset: RegionOperator, sc: SparkContext): RDD[GRECORD] = {
    logger.info("----------------PurgeRD executing..")
    val metaIdsList = executor.implement_md(metaDataset, sc).keys.distinct.collect
    executor.implement_rd(inputDataset, sc).filter((a: GRECORD) => metaIdsList.contains(a._1._1))
  }
}
