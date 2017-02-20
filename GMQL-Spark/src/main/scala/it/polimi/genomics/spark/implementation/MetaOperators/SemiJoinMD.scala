package it.polimi.genomics.spark.implementation.MetaOperators

import it.polimi.genomics.core.DataStructures.MetaJoinCondition.MetaJoinCondition
import it.polimi.genomics.core.DataStructures.MetaOperator
import it.polimi.genomics.core.DataTypes.MetaType
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

/**
 * Created by Abdulrahman Kaitoua on 25/06/15.
 */
object SemiJoinMD {

  private final val logger = LoggerFactory.getLogger(SemiJoinMD.getClass);

  @throws[SelectFormatException]
  def apply(executor : GMQLSparkExecutor, externalMeta : MetaOperator, joinCondition : MetaJoinCondition, inputDataset : MetaOperator, sc : SparkContext) : RDD[MetaType] = {

    logger.info("----------------SemiJoinMD executing..")

    val input = executor.implement_md(inputDataset, sc).map(x=>(x._2,x._1))
    val validInputId =
      executor
        .implement_md(externalMeta, sc)
        .filter(a => joinCondition.attributes.foldLeft(false)( (r,c) => r | a._2._1.endsWith(c.toString())) )
        .map(x=>(x._2,x._1))
        .join(input)
        .map(a => ((a._2._1, a._2._2), (a._1._1, 1)))
        .distinct()
        .reduceByKey((a , b ) => ( a._1, a._2+b._2))
        .filter(_._2._2 >= (joinCondition.attributes.size))
        .map(_._1._2).distinct()
        .collect
    input.filter(a => validInputId.contains(a._2)).map(x=>(x._2,x._1))
  }
}
