package it.polimi.genomics.spark.implementation.RegionsOperators

import it.polimi.genomics.core.DataStructures.RegionAggregate.RegionsToMeta
import it.polimi.genomics.core.DataStructures.RegionOperator
import it.polimi.genomics.core.DataTypes.MetaType
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.core.{GNull, GRecordKey, GValue}
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory


/**
  * Created by Abdulrahman Kaitoua on 07/07/15.
  */
object AggregateRD {
  private final val logger = LoggerFactory.getLogger(this.getClass);

  @throws[SelectFormatException]
  def apply(executor: GMQLSparkExecutor, aggregators: List[RegionsToMeta], inputDataset: RegionOperator, sc: SparkContext): RDD[MetaType] = {
    logger.info("----------------AggregateRD executing..")
    val ss: RDD[(GRecordKey, Array[GValue])] = executor.implement_rd(inputDataset, sc)

    val notAssociative = aggregators.flatMap(x => if (!x.associative) Some(x) else None)
    val associative = aggregators.flatMap(x => if (x.associative) Some(x) else None)

    val rddAssociative = if (associative.size > 0) {
      Aggregatable(ss, associative)
    } else sc.emptyRDD[MetaType]

    val rddNotAssociative = if (notAssociative.size > 0) {
      //extract the valute from the array
      ss.map(v => (v._1._1, notAssociative.foldLeft(Array[List[GValue]]())((z, a) => z :+ List(v._2(a.inputIndex)))))
        .reduceByKey((a, b) => a.zip(b).map(x => x._1 ++ x._2)).cache
        //evaluate aggregations
        .flatMap(v => notAssociative.zip(v._2).map(agg => (v._1, (agg._1.newAttributeName, agg._1.fun(agg._2).toString))))
    } else sc.emptyRDD[MetaType]

    rddAssociative.union(rddNotAssociative)

  }


  def Aggregatable(rdd: RDD[(GRecordKey, Array[GValue])], aggregator: List[RegionsToMeta]): RDD[MetaType] = {

    val extracted = rdd.map(x => (x._1._1, (aggregator.map(a => x._2(a.inputIndex)).toArray, (1, x._2.map(s=>if(s.isInstanceOf[GNull]) 0 else 1).iterator.toArray)))).cache
    extracted.reduceByKey { (x, y) => var i = -1;
      (aggregator.map { a => i += 1; a.fun(List(x._1(i), y._1(i))) }.toArray,(x._2._1 + y._2._1, x._2._2.zip(y._2._2).map(s=>s._1+s._2).iterator.toArray)) }
      .flatMap { x => var i = -1; aggregator.map { a => i += 1; (x._1, (a.newAttributeName, a.funOut(x._2._1(i), (x._2._2._1, if(x._2._2._2.size > 0) x._2._2._2(a.inputIndex) else 0)).toString)) } }
  }

}
