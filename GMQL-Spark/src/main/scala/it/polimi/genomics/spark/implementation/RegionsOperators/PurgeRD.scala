package it.polimi.genomics.spark.implementation.RegionsOperators

import it.polimi.genomics.core.DataStructures.{GMQLDatasetProfile, IROperator, MetaOperator, RegionOperator}
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
  def apply(operator: IROperator, executor: GMQLSparkExecutor, metaDataset: MetaOperator, inputDataset: RegionOperator, sc: SparkContext): RDD[GRECORD] = {
    logger.info("----------------PurgeRD executing..")
    val metaIdsList = executor.implement_md(metaDataset, sc).keys.distinct.collect
    val res = executor.implement_rd(inputDataset, sc).filter((a: GRECORD) => metaIdsList.contains(a._1._1))

    // Profile Estimation
    if( operator.requiresOutputProfile && inputDataset.outputProfile.isDefined) {
      val filtered = inputDataset.outputProfile.get.samples.filter( x=> metaIdsList.contains(x.ID) )
      operator.outputProfile = Some( new GMQLDatasetProfile(filtered) )
    }

    res
  }
}
