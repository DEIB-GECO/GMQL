package it.polimi.genomics.spark.implementation.MetaOperators

import it.polimi.genomics.core.DataStructures.MetaAggregate.MetaExtension
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
  def apply(executor: GMQLSparkExecutor, projectedAttributes: Option[List[String]], metaAggregator: Option[MetaExtension], all_but_flag:Boolean, inputDataset: MetaOperator, sc: SparkContext): RDD[MetaType] = {

    logger.info("----------------ProjectMD executing..")

    val input = executor.implement_md(inputDataset, sc)
    val filteredInput =
      if (projectedAttributes.isDefined) {
        val list = projectedAttributes.get
        input.filter{a =>
          val exists: Boolean = list.foldLeft(false)((x, y) =>
            x
              | a._2._1.endsWith("."+y)
              | a._2._1.startsWith(y+".")
              | a._2._1.equals(y))
          if(all_but_flag) !exists else exists
        }
      } else input

    if (metaAggregator.isDefined) {
      val agg = metaAggregator.get
      filteredInput.union(
        filteredInput
          .filter(in => agg.inputAttributeNames.foldLeft(false)( _ | in._2._1.endsWith(_)))
          .groupBy(x=>x._1)
          .map{x =>
            (x._1,
              (agg.newAttributeName ,
                agg.fun(x._2.groupBy(_._2._1)
                  .map(s=>if(agg.inputAttributeNames.foldLeft(false)( _ | s._1.endsWith(_)))s._2.map(_._2).toTraversable else Traversable()).toArray)))
        }
      )
    } else {
      filteredInput
    }
  }
}