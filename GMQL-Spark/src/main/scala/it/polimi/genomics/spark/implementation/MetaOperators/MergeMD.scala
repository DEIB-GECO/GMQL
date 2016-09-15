package it.polimi.genomics.spark.implementation.MetaOperators

import it.polimi.genomics.core.DataStructures.{MetaGroupOperator, MetaOperator}
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory


/**
 * Created by abdulrahman kaitoua on 07/06/15.
 */
object MergeMD {

  private final val logger = LoggerFactory.getLogger(MergeMD.getClass);

  @throws[SelectFormatException]
  def apply(executor : GMQLSparkExecutor, dataset : MetaOperator, groups : Option[MetaGroupOperator], sc : SparkContext) : RDD[MetaType] = {
    logger.info("----------------MergeMD executing..")

    val ds : RDD[MetaType] =
      executor.implement_md(dataset, sc)

      if (groups.isDefined) {
        val grouping = executor.implement_mgd(groups.get, sc);
        assignGroups(ds, grouping).distinct
      } else {
        //union of samples
        ds.map(m => (1L, (m._2._1, m._2._2))).distinct
      }
  }


  def assignGroups(dataset : RDD[MetaType], grouping : RDD[FlinkMetaGroupType2]) : RDD[MetaType] = {
    dataset.join(grouping).map{ x=> val g =x._2._2; val m = x._2._1
      (g, (m._1, m._2))
    }
  }
}
