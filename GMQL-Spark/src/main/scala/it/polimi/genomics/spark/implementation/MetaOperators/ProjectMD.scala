package it.polimi.genomics.spark.implementation.MetaOperators

import it.polimi.genomics.core.DataStructures.MetaAggregate.MetaAggregateStruct
import it.polimi.genomics.core.DataStructures.MetaOperator
import it.polimi.genomics.core.DataTypes.MetaType
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

/**
 * Created by abdulrahman kaitoua on 06/07/15.
 */
object ProjectMD {

  private final val logger = LoggerFactory.getLogger(ProjectMD.getClass);

  @throws[SelectFormatException]
  def apply(executor: GMQLSparkExecutor, projectedAttributes: Option[List[String]], metaAggregator: Option[MetaAggregateStruct], inputDataset: MetaOperator, sc: SparkContext): RDD[MetaType] = {

    logger.info("----------------ProjectMD executing..")

    val input = executor.implement_md(inputDataset, sc)
    val filteredInput =
      if (projectedAttributes.isDefined) {
        val list = projectedAttributes.get
        input.filter(a => list.contains(a._2._1))
      } else input

    if (metaAggregator.isDefined) {
      val agg = metaAggregator.get
      filteredInput.union(
        filteredInput.filter(in => agg.inputAttributeNames.contains(in._2._1)).groupBy(x=>x._1).map{x =>
          (x._1, (agg.newAttributeName , agg.fun(x._2.groupBy(_._2._1).map(s=>if(agg.inputAttributeNames.contains(s._1))s._2.map(_._2._2).toTraversable else Traversable()).toArray)))
        }
      )
    } else {
      filteredInput
    }
  }
}