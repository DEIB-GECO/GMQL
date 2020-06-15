package it.polimi.genomics.spark.implementation.MetaOperators

import it.polimi.genomics.core.DataStructures.{MetaGroupOperator, MetaOperator}
import it.polimi.genomics.core.DataTypes.MetaType
import it.polimi.genomics.core.Debug.EPDAG
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

/**
 * Created by abdulrahman kaitoua on 27/08/15.
 */
object CollapseMD {
  private final val logger = LoggerFactory.getLogger(CollapseMD.getClass);
  def apply(executor : GMQLSparkExecutor, grouping : Option[MetaGroupOperator], inputDataset : MetaOperator, sc : SparkContext) : (Float, RDD[MetaType]) = {

    logger.info("----------------CollapseMD executing..")

    val input = executor.implement_md(inputDataset, sc)._2

    val startTime: Float = EPDAG.getCurrentTime
    val out = if(grouping.isDefined){
      val groups = executor.implement_mgd(grouping.get, sc)._2
      input.join(groups).map{x=> val meta =x._2._1; val group =x._2._2; (group, meta)}
    } else input.map((meta) => (0L, meta._2))

    (startTime, out.distinct())

  }
}
