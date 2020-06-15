package it.polimi.genomics.spark.implementation.RegionsOperators

import it.polimi.genomics.core.DataStructures.RegionCondition.RegionCondition
import it.polimi.genomics.core.DataStructures.{MetaOperator, RegionOperator}
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.Debug.EPDAG
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by Abdulrahman Kaitoua on 25/05/15.
  */
object SelectRD {

  @throws[SelectFormatException]
  def apply(executor: GMQLSparkExecutor, regionCondition: Option[RegionCondition], filteredMeta: Option[MetaOperator], inputDataset: RegionOperator, sc: SparkContext): (Float, RDD[GRECORD]) = {
    PredicateRD.executor = executor

    val optimized_reg_cond = if (regionCondition.isDefined) Some(PredicateRD.optimizeConditionTree(regionCondition.get, false, filteredMeta, sc)) else None

    val input = executor.implement_rd(inputDataset, sc)._2
    val metaIdList = executor.implement_md(filteredMeta.get, sc)._2.keys.distinct.collect

    val startTime: Float = EPDAG.getCurrentTime

    val filteredRegion =
      if (filteredMeta.isDefined) {
       input.filter((a: GRECORD) => metaIdList.contains(a._1._1))
      } else input

    if (regionCondition.isDefined) {
      (startTime, filteredRegion.filter((region: GRECORD) => PredicateRD.applyRegionSelect(optimized_reg_cond.get, region)))
    } else {
      val res = filteredRegion
      (startTime, res)

    }
  }
}
