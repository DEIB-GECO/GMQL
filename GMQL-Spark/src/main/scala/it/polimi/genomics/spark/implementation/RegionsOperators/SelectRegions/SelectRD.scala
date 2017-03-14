package it.polimi.genomics.spark.implementation.RegionsOperators

import it.polimi.genomics.core.DataStructures.RegionCondition.RegionCondition
import it.polimi.genomics.core.DataStructures.{MetaOperator, RegionOperator}
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by Abdulrahman Kaitoua on 25/05/15.
  */
object SelectRD {

  @throws[SelectFormatException]
  def apply(executor: GMQLSparkExecutor, regionCondition: Option[RegionCondition], filteredMeta: Option[MetaOperator], inputDataset: RegionOperator, sc: SparkContext): RDD[GRECORD] = {
    PredicateRD.executor = executor

    val optimized_reg_cond = if (regionCondition.isDefined) Some(PredicateRD.optimizeConditionTree(regionCondition.get, false, filteredMeta, sc)) else None

    val input = executor.implement_rd(inputDataset, sc)
    val filteredRegion =
      if (filteredMeta.isDefined) {
        val metaIdList = executor.implement_md(filteredMeta.get, sc).keys.distinct.collect
        input.filter((a: GRECORD) => metaIdList.contains(a._1._1))
      } else input

    if (regionCondition.isDefined) {
      filteredRegion.filter((region: GRECORD) => PredicateRD.applyRegionSelect(optimized_reg_cond.get, region))
    } else {
      filteredRegion
    }
  }
}
