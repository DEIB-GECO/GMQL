package it.polimi.genomics.spark.implementation.MetaOperators.GroupOperator

import it.polimi.genomics.core.DataStructures.MetaJoinCondition.MetaJoinCondition
import it.polimi.genomics.core.DataStructures.MetaOperator
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory


/**
 * Created by abdulrahman kaitoua on 07/06/15.
 */
object MetaJoinMJD {

  final val logger = LoggerFactory.getLogger(this.getClass)

  //val hf : HashFunction = Hashing.sha256()

  @throws[SelectFormatException]
  def apply(executor : GMQLSparkExecutor, condition :  MetaJoinCondition, leftDataset : MetaOperator, rightDataset : MetaOperator, sc : SparkContext) : RDD[SparkMetaJoinType] = {


    import it.polimi.genomics.spark.implementation.MetaOperators.GroupOperator.MetaGroupMGD._
    val sampleWithGroup: RDD[(Long, Long)] =
      executor.implement_md(leftDataset, sc).MetaWithGroups(condition.attributes).map(x=>(x._2,x._1))// Key:group, value:sample
    val sampleWithGroup1: RDD[(Long, Long)] =
      executor.implement_md(rightDataset, sc).MetaWithGroups(condition.attributes).map(x=>(x._2,x._1))// Key:group, value:sample
    //(sampleID, groupID)


    val sampleSamplePairs: RDD[(Long, Long)] =
      sampleWithGroup
        .join(sampleWithGroup1).flatMap(x=> if(x._2._1 == x._2._2)None else Some( x._2))
        .distinct(1)(new Ordering[(Long,Long)](){ def compare(left:(Long,Long),right:(Long,Long))={(left._1+left._2) compare (right._1+right._2)} }) //key: ref Sample ID, value: exp sample ID

    val groupedPairsIDs: RDD[(Long, Array[Long])] = sampleSamplePairs.groupByKey().map(x=>(x._1,x._2.toArray))
    //(sampleID, sampleID)

/*
        val rdd: RDD[(Long,( String, String))] =
      executor.implement_md(leftDataset, sc)
        .union(
          executor.implement_md(rightDataset, sc)
        )
        .filter((v : MetaType) => condition.attributes.contains(v._2._1))


    import it.polimi.genomics.spark.implementation.MetaOperators.GroupOperator.MetaGroupMGD._
    val sampleWithGroup: RDD[(Long, Long)] =
      rdd.MetaWithGroups(condition.attributes).flatMap(x=>x._2.map(s=>(s,x._1)))
    //(sampleID, groupID)


    val sampleSamplePairs =
      sampleWithGroup
        .join(sampleWithGroup).map(x=>x._2)
        .distinct
    //(sampleID, sampleID)
*/
    groupedPairsIDs
  }
}
