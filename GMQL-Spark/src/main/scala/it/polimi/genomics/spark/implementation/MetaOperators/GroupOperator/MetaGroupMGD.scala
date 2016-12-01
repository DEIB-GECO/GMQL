package it.polimi.genomics.spark.implementation.MetaOperators.GroupOperator

import java.nio.charset.StandardCharsets

import com.google.common.hash.Hashing
import it.polimi.genomics.core.DataStructures.MetaGroupByCondition.MetaGroupByCondition
import it.polimi.genomics.core.DataStructures.MetaOperator
import it.polimi.genomics.core.DataTypes.{FlinkMetaGroupType2, MetaType}
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.annotation.tailrec

/**
 * Created by abdulrahman kaitoua on 24/05/15.
 */
object MetaGroupMGD {
  @throws[SelectFormatException]
  def apply(executor: GMQLSparkExecutor, condition: MetaGroupByCondition, inputDataset: MetaOperator, sc: SparkContext): RDD[FlinkMetaGroupType2] = {
    executor.implement_md(inputDataset, sc).MetaWithGroups(condition.attributes)
  }

  implicit class MetaGroup(rdd: RDD[MetaType]) {
    def MetaWithGroups(condition: List[String]): RDD[FlinkMetaGroupType2] = {

      rdd.flatMap { x => //val key = x._2._1.substring({val pos = x._2._1.lastIndexOf('.'); if (pos < 0) 0 else pos});
        val matchedKey = condition.filter(k => x._2._1.endsWith(k))
        matchedKey
//        if (!condition.foldLeft(false)( _ | key.endsWith(_))) None
        if (matchedKey.size == 0) None
//        else Some((x._1, (key, x._2._2)))
        else Some((x._1, (matchedKey(0), x._2._2)))
      }.groupByKey()

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
