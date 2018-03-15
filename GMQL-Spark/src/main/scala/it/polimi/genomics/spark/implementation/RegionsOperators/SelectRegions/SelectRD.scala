package it.polimi.genomics.spark.implementation.RegionsOperators

import it.polimi.genomics.core.DataStructures.RegionCondition.RegionCondition
import it.polimi.genomics.core.DataStructures._
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.{GRecordKey, GValue}
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import it.polimi.genomics.spark.implementation.RegionsOperators.SelectIRD.logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by Abdulrahman Kaitoua on 25/05/15.
  */
object SelectRD {

  @throws[SelectFormatException]
  def apply(operator: IROperator, executor: GMQLSparkExecutor, regionCondition: Option[RegionCondition], filteredMeta: Option[MetaOperator], inputDataset: RegionOperator, sc: SparkContext): RDD[GRECORD] = {
    PredicateRD.executor = executor

    val optimized_reg_cond = if (regionCondition.isDefined) Some(PredicateRD.optimizeConditionTree(regionCondition.get, false, filteredMeta, sc)) else None

    val input = executor.implement_rd(inputDataset, sc)

    var metaIdList: Option[Array[Long]] = None

    val filteredRegion =
      if (filteredMeta.isDefined) {
        metaIdList = Some(executor.implement_md(filteredMeta.get, sc).keys.distinct.collect)
        input.filter((a: GRECORD) => metaIdList.get.contains(a._1._1))
      } else input

    val result =
    if (regionCondition.isDefined) {
      filteredRegion.filter((region: GRECORD) => PredicateRD.applyRegionSelect(optimized_reg_cond.get, region))
    } else {
      filteredRegion
    }

    // Profile Estimation: empty if regionCondition, filtered if metaCondition
    if (operator.requiresOutputProfile && regionCondition.isEmpty && inputDataset.outputProfile.isDefined) {
      val sampProfiles = inputDataset.outputProfile.get.samples.filter(x => metaIdList.contains(x.ID) )
      operator.outputProfile = Some(new GMQLDatasetProfile(sampProfiles))

      println("\n\n Resulting Profile has: " + operator.outputProfile.get.get(Feature.NUM_SAMP) )
    }

    result
  }
}
