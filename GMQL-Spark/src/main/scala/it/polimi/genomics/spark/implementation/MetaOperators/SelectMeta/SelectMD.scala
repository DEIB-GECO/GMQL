package it.polimi.genomics.spark.implementation.MetaOperators.SelectMeta

import it.polimi.genomics.core.DataStructures.MetaOperator
import it.polimi.genomics.core.DataStructures.MetadataCondition._
import it.polimi.genomics.core.DataTypes.MetaType
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

/*
created by abdulrahman Kaitoua on 05/05/15.
 */
object SelectMD {
  private final val logger = LoggerFactory.getLogger(SelectMD.getClass)

  @throws[SelectFormatException]
  def apply(executor : GMQLSparkExecutor,
            metaCondition: MetadataCondition,
            inputDataset: MetaOperator,
            sc : SparkContext) : RDD[MetaType] = {

    logger.info("----------------SELECTMD executing..")

    val input = executor.implement_md(inputDataset, sc).cache()

    input
      .groupByKey()
      .filter(x => metaSelection.build_set_filter(metaCondition)(x._2))
      .flatMap(x=> for(p <- x._2) yield (x._1, p))
      .cache()

  }
  object metaSelection extends MetaSelection
}