package it.polimi.genomics.spark.implementation.RegionsOperators

import it.polimi.genomics.core.DataStructures.{MetaOperator, RegionOperator}
import it.polimi.genomics.core.DataTypes.GRECORD
import it.polimi.genomics.core.Debug.EPDAG
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
  def apply(executor: GMQLSparkExecutor, metaDataset: MetaOperator, inputDataset: RegionOperator, sc: SparkContext): (Float, RDD[GRECORD]) = {
    logger.info("----------------PurgeRD executing..")
    val metaIdsList = executor.implement_md(metaDataset, sc)._2.keys.distinct.collect

    val res = executor.implement_rd(inputDataset, sc)._2
    val startTime: Float = EPDAG.getCurrentTime

    (startTime, res.filter((a: GRECORD) => metaIdsList.contains(a._1._1)))
  }
}
