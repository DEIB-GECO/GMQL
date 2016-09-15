package it.polimi.genomics.spark.implementation.MetaOperators.SelectMeta

import it.polimi.genomics.core.DataStructures.MetaOperator
import it.polimi.genomics.core.DataTypes.MetaType
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

/**
 * Created by abdulrahman Kaitoua on 25/05/15.
 */
object StoreMD {

  private final val logger = LoggerFactory.getLogger(StoreMD.getClass);

  @throws[SelectFormatException]
  def apply(executor : GMQLSparkExecutor, path: String, value: MetaOperator, sc : SparkContext): RDD[MetaType] = {
    logger.info("----------------STOREMD executing..")
    executor.implement_md(value, sc)
  }
}
