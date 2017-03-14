package it.polimi.genomics.spark.implementation.RegionsOperators

import it.polimi.genomics.core.DataStructures.{MetaGroupOperator, RegionOperator}
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.GRecordKey
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

/**
 * Created by abdulrahman kaitoua on 04/07/15.
 */
object MergeRD {

  private final val logger = LoggerFactory.getLogger(this.getClass);

  @throws[SelectFormatException]
  def apply(executor : GMQLSparkExecutor, dataset : RegionOperator, groups : Option[MetaGroupOperator], sc : SparkContext) : RDD[GRECORD] = {
    logger.info("----------------Merge executing..")

    val ds : RDD[GRECORD] =
      executor.implement_rd(dataset, sc)

    val groupedDs : RDD[GRECORD] =
      if (groups.isDefined) {
        //group
        val grouping = executor.implement_mgd(groups.get, sc);
        assignGroups(ds, grouping)
      } else {
        //union of samples
        ds.map((r) => {
          (new GRecordKey(1L, r._1._2, r._1._3, r._1._4, r._1._5), r._2)
        })
      }

    groupedDs
  }

  def assignGroups(dataset : RDD[GRECORD], grouping : RDD[FlinkMetaGroupType2]) : RDD[GRECORD] = {
    dataset.map(x => (x._1._1,x)).join(grouping).map{ x=> val r = x._2._1; val g = x._2._2
      (new GRecordKey(g, r._1._2, r._1._3, r._1._4, r._1._5), r._2)
    }
  }
}
