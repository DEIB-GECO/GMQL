package it.polimi.genomics.spark.implementation.MetaOperators

import it.polimi.genomics.core.DataStructures.MetaGroupByCondition.MetaGroupByCondition
import it.polimi.genomics.core.DataStructures.RegionAggregate.RegionsToMeta
import it.polimi.genomics.core.DataStructures.{MetaOperator, RegionOperator}
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.{GNull, GValue}
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
  def apply(executor: GMQLSparkExecutor, groupingKeys : MetaGroupByCondition, aggregates : List[RegionsToMeta], newGroupNameInMeta : String, inputDataset : MetaOperator, regionDataset : RegionOperator, sc: SparkContext): RDD[MetaType] = {
    logger.info("----------------GroupMD executing..")
    //INPUT
    val ds : RDD[MetaType] =
      executor.implement_md(inputDataset, sc)

    val rDs : RDD[GRECORD] =
      executor.implement_rd(regionDataset, sc)

    import it.polimi.genomics.spark.implementation.MetaOperators.GroupOperator.MetaGroupMGD._

    //(sampleId, GroupIds)
    val groups : RDD[(Long, Long)] =
      ds.MetaWithGroups(groupingKeys.attributes)

    //EXECUTION
    //Metadata : (groupid, sampleid, groupName)
    val groupingMeta: RDD[(Long,(Long,String))] =
      groups.map{s => (s._2,(s._1, newGroupNameInMeta))}

    val aggregationIndexes : List[Int] =
      aggregates.foldLeft(List() : List[Int])((z, a) => z :+ a.inputIndex)

    //(GroupId, Array[field to be aggreagated in the same order as in aggregation parameter], SampleId)
    val cleanedGroupedRegions : RDD[(Long, Array[List[GValue]])] =
      rDs.map(x=>(x._1._1,x._2)).join(groups).map{x=>
        (x._2._2, aggregationIndexes.foldLeft(new Array[List[GValue]](0))((z, a) => z :+ List(x._2._1(a))))
      }

    //MetaData : (GroupId, attributeName, value)
    //group regions by group ID, aggregate values in a bag
    //for each group apply aggregation functions and create metadata
    //(groupId, attributeName, Value)
    val newAggregationMetaByGroup : RDD[(Long,( String, String))] =
      cleanedGroupedRegions
        .reduceByKey{(l,r) =>(l.zip(r).map(a=> a._1 ++ a._2))}
        .flatMap{a => a._2.zip(aggregates).map{n =>
          val fun = n._2.fun(n._1);
          val count = (n._1.length, n._1.foldLeft(0)((x,y)=> if (y.isInstanceOf[GNull]) x+0 else x+1));
          (a._1, (n._2.newAttributeName, n._2.funOut(fun, count).toString))}}

    //MetaData : (SampleId, attributeName, Value)
    val aggregationMeta : RDD[(Long, (String, String))] =
      groupingMeta.join(newAggregationMetaByGroup).map{x=> (x._2._1._1, (x._2._2._1, x._2._2._2))}

    //CLOSING
    //merge the 3 meta data sets and output
    aggregationMeta.union(groupingMeta.map(x=> (x._2._1,(x._2._2,x._1.toString)))).union(ds)
  }
}
