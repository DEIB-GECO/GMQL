package it.polimi.genomics.flink.FlinkImplementation.operator.meta

import it.polimi.genomics.core.DataStructures.MetaGroupByCondition.MetaGroupByCondition
import it.polimi.genomics.core.DataStructures.RegionAggregate.RegionsToMeta
import it.polimi.genomics.core.DataStructures.{MetaOperator, RegionOperator}
import it.polimi.genomics.core.DataTypes.{FlinkMetaGroupType2, FlinkMetaType, FlinkRegionType}
import it.polimi.genomics.core.{GNull, GValue}
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.flink.FlinkImplementation.FlinkImplementation
import it.polimi.genomics.flink.FlinkImplementation.operator.metaGroup.MetaGroupMGD
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

/**
 * Created by michelebertoni on 11/05/15.
 */
object GroupMD {

  final val logger = LoggerFactory.getLogger(this.getClass)


  @throws[SelectFormatException]
  def apply(executor: FlinkImplementation, groupingKeys : MetaGroupByCondition, aggregates : List[RegionsToMeta], newGroupNameInMeta : String, inputDataset : MetaOperator, regionDataset : RegionOperator, env: ExecutionEnvironment): DataSet[FlinkMetaType] = {

    //logger.warn("Executing GroupMD")

    //INPUT
    val ds : DataSet[FlinkMetaType] =
      executor.implement_md(inputDataset, env)

    val rDs : DataSet[FlinkRegionType] =
      executor.implement_rd(regionDataset, env)

    //(sampleId, List(GroupIds))
    val groups : DataSet[(Long, Long)] =
      MetaGroupMGD.execute(groupingKeys, ds)


    //EXECUTION
    //Metadata : (SampleId, groupName, groupId)
    val groupingMeta: DataSet[FlinkMetaType] =
      createGroupMetaData(groups, newGroupNameInMeta)

    val aggregationIndexes : List[Int] =
      aggregates.foldLeft(List() : List[Int])((z, a) => z :+ a.inputIndex)

    //(GroupId, Array[field to be aggreagated in the same order as in aggregation parameter], SampleId)
    val cleanedGroupedRegions : DataSet[(Long, Array[List[GValue]])] =
      rDs.joinWithTiny(groups).where(0).equalTo(0){
        (r : FlinkRegionType, g : (Long, Long), out : Collector[(Long, Array[List[GValue]])]) => {
          val cleanedR =
            (aggregationIndexes.foldLeft(new Array[List[GValue]](0))((z, a) => z :+ List(r._6(a))))
          out.collect((g._2, cleanedR))
        }
      }

    //MetaData : (GroupId, attributeName, value)
    val newAggregationMetaByGroup : DataSet[(String, String, String)] =
      cleanedGroupedRegions
        //group regions by group ID
        .groupBy(0)
        //aggregate values in a bag
        .reduce((l : (Long, Array[List[GValue]]), r : (Long, Array[List[GValue]])) => {
          (l._1,
            l._2.zip(r._2)
            .map((a : (List[GValue], List[GValue])) => {
              a._1 ++ a._2
            })
          )
        })
        //for each group apply aggregation functions and create metadata
        //(groupId, attributeName, Value)
        .flatMap((a : (Long, Array[List[GValue]]), out : Collector[(String, String, String)]) => {
          a._2.zip(aggregates)
            .map((n : (List[GValue], RegionsToMeta)) => {
              val fun = n._2.fun(n._1);
              val count = (n._1.length, n._1.foldLeft(0)((x,y)=> if (y.isInstanceOf[GNull]) x+0 else x+1));
              out.collect((a._1.toString, n._2.newAttributeName, n._2.funOut(fun,count).toString))
            })
        })

    //MetaData : (SampleId, attributeName, Value)
    val aggregationMeta : DataSet[(Long, String, String)] =
      groupingMeta.joinWithHuge(newAggregationMetaByGroup).where(2).equalTo(0){
          (g : FlinkMetaType, a : (String, String, String)) => {
            (g._1, a._2, a._3)
          }
        }

    //CLOSING
    //merge the 3 meta data sets and output
    aggregationMeta.union(groupingMeta).union(ds)

  }

  def createGroupMetaData(groups : DataSet[FlinkMetaGroupType2], newGroupNameInMeta : String) : DataSet[FlinkMetaType] = {
    groups.map((s) => {
      (s._1, newGroupNameInMeta, s._2.toString)
    })
  }

}
