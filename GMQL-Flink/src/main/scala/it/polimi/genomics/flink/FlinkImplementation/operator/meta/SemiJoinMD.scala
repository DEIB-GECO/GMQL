package it.polimi.genomics.flink.FlinkImplementation.operator.meta

import it.polimi.genomics.core.DataStructures.MetaJoinCondition.MetaJoinCondition
import it.polimi.genomics.core.DataStructures.MetaOperator
import it.polimi.genomics.core.DataTypes.FlinkMetaType
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.flink.FlinkImplementation.FlinkImplementation
import org.apache.flink.api.scala._
import org.slf4j.LoggerFactory

/**
 * Created by michelebertoni on 05/05/15.
 */
object SemiJoinMD {

  final val logger = LoggerFactory.getLogger(this.getClass)


  @throws[SelectFormatException]
  def apply(executor : FlinkImplementation, externalMeta : MetaOperator, joinCondition : MetaJoinCondition, inputDataset : MetaOperator, env : ExecutionEnvironment) : DataSet[FlinkMetaType] = {

    //logger.warn("Executing SemiJoinMD")

    val input = executor.implement_md(inputDataset, env)
    val validInputId =
      executor
        .implement_md(externalMeta, env)
        .filter((a : FlinkMetaType) => joinCondition.attributes.contains(a._2))
        .join(input)
        .where(1,2)
        .equalTo(1,2)
        .map((a : (FlinkMetaType, FlinkMetaType)) => (a._1._1, a._2._1, a._1._2, 1))
        .distinct(0,1,2)
        .groupBy(0,1)
        .reduce((a : (Long, Long, String, Int), b : (Long, Long, String, Int)) => (a._1, a._2, a._3, a._4+b._4))
        .filter(_._4.equals(joinCondition.attributes.size))
        .map(_._2)
        .collect
    input.filter((a : FlinkMetaType) => validInputId.contains(a._1))
  }
}
