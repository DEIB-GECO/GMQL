package it.polimi.genomics.spark.implementation.MetaOperators

import it.polimi.genomics.core.DataStructures.MetaAggregate.MetaAggregateFunction
import it.polimi.genomics.core.DataStructures.MetaGroupByCondition.MetaGroupByCondition
import it.polimi.genomics.core.DataStructures.{MetaOperator, RegionOperator}
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

/**
 * Created by abdulrahman kaitoua on 11/07/15.
 */
object GroupMD {

  private final val logger = LoggerFactory.getLogger(GroupMD.getClass);

  @throws[SelectFormatException]
  def apply(executor: GMQLSparkExecutor, groupingKeys : MetaGroupByCondition, aggregates : Option[List[MetaAggregateFunction]], newGroupNameInMeta : String, inputDataset : MetaOperator, regionDataset : RegionOperator, sc: SparkContext): RDD[MetaType] = {
    logger.info("----------------GroupMD executing..")
    //INPUT
    val ds : RDD[MetaType] =
      executor.implement_md(inputDataset, sc)

    /*val rDs : RDD[GRECORD] =
      executor.implement_rd(regionDataset, sc)*/

    import it.polimi.genomics.spark.implementation.MetaOperators.GroupOperator.MetaGroupMGD._

    //(sampleId, GroupIds)
    /*val groups : RDD[(Long, Long)] =
      ds.MetaWithGroups(groupingKeys.attributes)*/

    //remove newGroupNameInMeta from meta if any
    val dsWithoutGroupName: RDD[MetaType] =
      ds.filter(!_._2._1.equals(newGroupNameInMeta))

    //Groups that contains groupingKeys
    //(sampleId, GroupIds)
    val preGroups : RDD[(Long, Long)] =
      dsWithoutGroupName.MetaWithGroups(groupingKeys.attributes)

    //Group that does NOT have groupingKey, groupId = 0
    //(sampleId, groupId)
    val zeroGroup: RDD[(Long, Long)] = ds.map(_._1).distinct().subtract(preGroups.map(_._1).distinct()).map((_,0l))

    //All groups
    //(sampleId, groupId)
    val groups: RDD[(Long, Long)] =
      preGroups.map(x=> (x._2,x._1)).groupByKey.zipWithIndex.flatMap{ case (group, id) => group._2.map( sample=> (id+1, sample)) }.map(x=> (x._2,x._1)).union(zeroGroup)


    //EXECUTION
    //Metadata : (groupid, sampleid, groupName)
    val groupingMeta: RDD[(Long,(Long,String))] =
      groups.map{s => (s._2,(s._1, newGroupNameInMeta))}

    /*val aggregationIndexes : List[Int] =
      aggregates.foldLeft(List() : List[Int])((z, a) => z :+ a.inputIndex)*/

    //(GroupId, Array[field to be aggreagated in the same order as in aggregation parameter], SampleId)
    /*val cleanedGroupedRegions : RDD[(Long, Array[List[GValue]])] =
      rDs.map(x=>(x._1._1,x._2)).join(groups).map{x=>
        (x._2._2, aggregationIndexes.foldLeft(new Array[List[GValue]](0))((z, a) => z :+ List(x._2._1(a))))
      }*/

    //MetaData : (GroupId, attributeName, value)
    //group regions by group ID, aggregate values in a bag
    //for each group apply aggregation functions and create metadata
    //(groupId, attributeName, Value)
    /*val newAggregationMetaByGroup : RDD[(Long,( String, String))] =
      cleanedGroupedRegions
        .reduceByKey{(l,r) =>(l.zip(r).map(a=> a._1 ++ a._2))}
        .flatMap{a => a._2.zip(aggregates).map{n =>
          val fun = n._2.fun(n._1);
          val count = (n._1.length, n._1.foldLeft(0)((x,y)=> if (y.isInstanceOf[GNull]) x+0 else x+1));
          (a._1, (n._2.newAttributeName, n._2.funOut(fun, count).toString))}}*/

    if (aggregates.isDefined && !aggregates.get.isEmpty) {
      val newAggregationMetaByGroup: RDD[(Long, (String, String))] =
        aggregates.get.map { a =>
          if (a.inputName.isEmpty)
            dsWithoutGroupName.join(groups).map(x=> (x._2._2, x._1.toString))
              .groupByKey
              .map{x=>(x._1, (a.newAttributeName, x._2.toArray.distinct.length.toString))}
          else
          dsWithoutGroupName.join(groups).filter(in => in._2._1._1.equals(a.inputName)).map { x => (x._2._2, x._2._1._2) }
            .groupBy(_._1)
            .map { n => (n._1, (a.newAttributeName, a.fun(n._2.groupBy(_._1).map(s => s._2.map(_._2).toTraversable).toArray))) }
        }.reduce(_ union _)

      //MetaData : (SampleId, attributeName, Value)
      val aggregationMeta: RDD[(Long, (String, String))] =
        groupingMeta.join(newAggregationMetaByGroup).map { x => (x._2._1._1, (x._2._2._1, x._2._2._2)) }

      //CLOSING
      //merge the 3 meta data sets and output
      aggregationMeta.union(groupingMeta.map(x => (x._2._1, (x._2._2, x._1.toString)))).union(dsWithoutGroupName)
    }
    else {
      groupingMeta.map(x => (x._2._1, (x._2._2, x._1.toString))).union(dsWithoutGroupName)
    }
  }
}
