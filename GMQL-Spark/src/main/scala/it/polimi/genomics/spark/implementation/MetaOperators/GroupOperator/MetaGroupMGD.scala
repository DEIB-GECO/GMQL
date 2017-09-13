package it.polimi.genomics.spark.implementation.MetaOperators.GroupOperator

import java.nio.charset.StandardCharsets

import com.google.common.hash.Hashing
import it.polimi.genomics.core.DataStructures.MetaGroupByCondition.MetaGroupByCondition
import it.polimi.genomics.core.DataStructures.MetaJoinCondition.{AttributeEvaluationStrategy, Default, Exact, FullName}
import it.polimi.genomics.core.DataStructures.MetaOperator
import it.polimi.genomics.core.DataTypes.{FlinkMetaGroupType2, MetaType}
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Created by abdulrahman kaitoua on 24/05/15.
 */
object MetaGroupMGD {
  @throws[SelectFormatException]
  def apply(executor: GMQLSparkExecutor, condition: MetaGroupByCondition, inputDataset: MetaOperator, sc: SparkContext): RDD[FlinkMetaGroupType2] = {
    executor.implement_md(inputDataset, sc).MetaWithGroups(condition.attributes)
  }

  implicit class MetaGroup(rdd: RDD[MetaType]) {
    def MetaWithGroups(condition: List[AttributeEvaluationStrategy]): RDD[FlinkMetaGroupType2] = {

//      rdd.flatMap { x => //val key = x._2._1.substring({val pos = x._2._1.lastIndexOf('.'); if (pos < 0) 0 else pos});
//        val matchedKey = condition.filter(k => /*x._2._1.endsWith(k)*/ x._2._1.equals(k)||x._2._1.endsWith("."+k))
////        if (!condition.foldLeft(false)( _ | key.endsWith(_))) None
//        if (matchedKey.size == 0) None
////        else Some((x._1, (key, x._2._2)))
//        else Some((x._1, (matchedKey(0), x._2._2)))
//      }
      //
      rdd.filter{ x =>
        condition.foldLeft(false)((r,c) =>
          r | {
            if (c.isInstanceOf[FullName]) {
              (x._2._1.equals(c.asInstanceOf[FullName].attribute.toString()) || x._2._1.endsWith("." + c.asInstanceOf[FullName].attribute.toString()))
            }
            else if (c.isInstanceOf[Exact]) {
              x._2._1.equals(c.asInstanceOf[Exact].attribute.toString())
            }
            else {
              (x._2._1.equals(c.asInstanceOf[Default].attribute.toString()) || x._2._1.endsWith("." + c.asInstanceOf[Default].attribute.toString()))
            }
          }
        )
      }.map{x=>
        condition.flatMap{att=>
          if (att.isInstanceOf[FullName]) {
            if (x._2._1.equals(att.asInstanceOf[FullName].attribute) || x._2._1.endsWith("." + att.asInstanceOf[FullName].attribute))
              Some((x._1, (x._2._1, x._2._2)))
            else None
          }
          else if (att.isInstanceOf[Exact]) {
            if (x._2._1.equals(att.asInstanceOf[Exact].attribute))
              Some((x._1, (att.asInstanceOf[Exact].attribute.toString(), x._2._2)))
            else None
          }
          else {
            if (x._2._1.equals(att.asInstanceOf[Default].attribute) || x._2._1.endsWith("." + att.asInstanceOf[Default].attribute))
              Some((x._1, (att.asInstanceOf[Default].attribute.toString(), x._2._2)))
            else None
          }
        }.head
      }
        .groupByKey()
        .flatMap { x =>
          val itr = x._2.toList.distinct
          if (!itr.iterator.hasNext) None
          else {
            Some(x._1, Hashing.md5.newHasher().putString(
              itr.groupBy(_._1).map(d => (d._1, d._2.map(e => e._2).sorted.mkString("§"))).toList.sortBy(_._1).mkString("£")
              , StandardCharsets.UTF_8).hash().asLong)
          }
      }
      //output is (SampleID, Group)

    }
  }
}
