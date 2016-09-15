package it.polimi.genomics.flink.FlinkImplementation.operator.meta

import it.polimi.genomics.core.DataStructures.MetaAggregate.MetaAggregateStruct
import it.polimi.genomics.core.DataStructures.MetaOperator
import it.polimi.genomics.core.DataTypes.FlinkMetaType
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.flink.FlinkImplementation.FlinkImplementation
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import scala.collection.immutable.HashSet

/**
 * Created by michelebertoni on 06/05/15.
 */
object ProjectMD {

  final val logger = LoggerFactory.getLogger(this.getClass)

  @throws[SelectFormatException]
  def apply(executor: FlinkImplementation, projectedAttributes: Option[List[String]], metaAggregator: Option[MetaAggregateStruct], inputDataset: MetaOperator, env: ExecutionEnvironment): DataSet[FlinkMetaType] = {

    //logger.warn("Executing ProjectMD")

    val input = executor.implement_md(inputDataset, env)
    val filteredInput =
      if (projectedAttributes.isDefined) {
        val list = projectedAttributes.get.foldLeft(new HashSet[String]())((z, v) => {z + v})
        input.filter((a: FlinkMetaType) => list.contains(a._2))
      } else {
        input
      }

    val output =
      if (metaAggregator.isDefined) {
        val agg = metaAggregator.get
        val inputAttributesName = agg.inputAttributeNames.foldLeft(new HashSet[String]())((z, v) => {z + v})
        filteredInput.union(
          filteredInput
            .flatMap((v, out: Collector[(Long, String, Traversable[String])]) => {
              if(inputAttributesName.contains(v._2)){
                out.collect((v._1, v._2, Traversable(v._3)))
              }
            })
            .groupBy(0)
            .reduceGroup(x => {
              val y = x.toTraversable
              val grouped : Map[String, (Long, String, Traversable[String])] =
                y
                  .groupBy(_._2)
                  .map((v) => (v._1, v._2.reduce((a,b) => {
                    (a._1, a._2, a._3 ++ b._3)
                  })))
                  .toMap
              val array : Array[Traversable[String]] =
                agg
                  .inputAttributeNames
                  .map((v) => {
                    grouped.getOrElse(v, (0L, "", Traversable()))._3
                  })
                  .toArray
            (y.head._1, agg.newAttributeName , agg.fun(array))
          })
        )
      } else {
        filteredInput
      }
    output
  }
}