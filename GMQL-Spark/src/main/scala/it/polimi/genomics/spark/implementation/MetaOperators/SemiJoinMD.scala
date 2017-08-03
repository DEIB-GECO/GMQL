package it.polimi.genomics.spark.implementation.MetaOperators

import it.polimi.genomics.core.DataStructures.MetaJoinCondition.{Default, Exact, FullName, MetaJoinCondition}
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

    val input = executor.implement_md(inputDataset, sc)/*.map(x=>(x._2,x._1))*/

    val externalValues = executor
      .implement_md(externalMeta, sc)
      .filter(a => joinCondition.attributes.foldLeft(false)( (r,c) => r | /*a._2._1.endsWith(c.toString())*/
        {
          if (c.isInstanceOf[FullName]) (a._2._1.equals(c.asInstanceOf[FullName].attribute)||a._2._1.endsWith("."+c.asInstanceOf[FullName].attribute))
          else if (c.isInstanceOf[Exact]) (a._2._1.equals(c.asInstanceOf[Exact].attribute))
          else (a._2._1.equals(c.asInstanceOf[Default].attribute)||a._2._1.endsWith("."+c.asInstanceOf[Default].attribute))
        }))
      .map(x=>/*(x._2,x._1)*/
        joinCondition.attributes.map { att =>
          if (att.isInstanceOf[FullName] || att.isInstanceOf[Exact]) ((x._2._1, x._2._2), x._1)
          else if (x._2._1.equals(att.asInstanceOf[Default].attribute) || x._2._1.endsWith("." + att.asInstanceOf[Default].attribute))
            ((att.asInstanceOf[Default].attribute, x._2._2), x._1)
          else ((x._2._1, x._2._2), x._1)
        }.head)

    val inputValues = input.map(x=>
      joinCondition.attributes.flatMap { att =>
        if (att.isInstanceOf[FullName] || att.isInstanceOf[Exact]) Some(((x._2._1, x._2._2), x._1))
        else if (x._2._1.equals(att.asInstanceOf[Default].attribute) || x._2._1.endsWith("." + att.asInstanceOf[Default].attribute))
          Some(((att.asInstanceOf[Default].attribute.toString(), x._2._2), x._1))
        else Some(((x._2._1, x._2._2), x._1))
      }.head)


    val validInputId =if(joinCondition.negation){
      val externalColletion = externalValues.collect()
      inputValues.filter(a => joinCondition.attributes.foldLeft(false)( (r,c) => r | /*a._2._1.endsWith(c.toString())*/
        {
          if (c.isInstanceOf[FullName]) (a._1._1.equals(c.asInstanceOf[FullName].attribute)||a._1._1.endsWith("."+c.asInstanceOf[FullName].attribute))
          else if (c.isInstanceOf[Exact]) (a._1._1.equals(c.asInstanceOf[Exact].attribute))
          else (a._1._1.equals(c.asInstanceOf[Default].attribute)||a._1._1.endsWith("."+c.asInstanceOf[Default].attribute))
        }))
          .flatMap{ record =>
        if(externalColletion.filter{x=> !(x._1._1 == record._1._1 && x._1._2 != record._1._2)}.size == 0) Some(record._2) else None
      }.collect()
    }else
      externalValues
        .join(inputValues)
        .map(a => ((a._2._1, a._2._2), (a._1._1, 1)))
        .distinct()
        .reduceByKey((a , b ) => ( a._1, a._2+b._2))
        .filter(_._2._2 >= (joinCondition.attributes.size))
        .map(_._1._2).distinct()
        .collect

    input.map(x=>(x._2,x._1)).filter(a => validInputId.contains(a._2)).map(x=>(x._2,x._1))
  }
}
