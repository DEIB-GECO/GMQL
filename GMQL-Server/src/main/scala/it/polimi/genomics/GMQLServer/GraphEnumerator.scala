package it.polimi.genomics.GMQLServer

import it.polimi.genomics.core.DataStructures.CoverParameters.CoverFlag._
import it.polimi.genomics.core.DataStructures.CoverParameters.CoverParam
import it.polimi.genomics.core.DataStructures.GroupMDParameters.Direction._
import it.polimi.genomics.core.DataStructures.GroupMDParameters.TopParameter
import it.polimi.genomics.core.DataStructures.JoinParametersRD.JoinQuadruple
import it.polimi.genomics.core.DataStructures.JoinParametersRD.RegionBuilder._
import it.polimi.genomics.core.DataStructures.MetaAggregate.MetaAggregateStruct
import it.polimi.genomics.core.DataStructures.RegionCondition.RegionCondition
import it.polimi.genomics.core.DataStructures._
import it.polimi.genomics.core.DataStructures.MetaGroupByCondition.MetaGroupByCondition
import it.polimi.genomics.core.DataStructures.RegionAggregate.{RegionsToRegion, RegionExtension, RegionsToMeta}
import it.polimi.genomics.core.DataStructures.MetaJoinCondition.MetaJoinCondition

import scala.collection.immutable.HashMap

/**
 * Created by michelebertoni on 16/07/15.
 */
object GraphEnumerator {
  def apply(graph : List[IRVariable]) : List[(IROperator, Set[IROperator])] = {

    def numAssigner(list : List[IROperator], pos : Int, current : Option[IROperator], acc : HashMap[IROperator, (Int, Set[IROperator])]) : HashMap[IROperator, (Int, Set[IROperator])] = {
      if(list.size.equals(0)){
        acc
      } else {
        numAssigner(list.tail, pos+1, current, acc + ((list.head, (pos, {if(current.isDefined) Set(current.get) else Set[IROperator]() } ))) )
      }
    }

    def enumerateNodesHelper(list : List[IROperator], number : Int, acc : HashMap[IROperator, (Int, Set[IROperator])]) : HashMap[IROperator, (Int, Set[IROperator])] = {
      if(list.size.equals(0)){
        acc
      } else {
        val children : List[IROperator] = {
          list.head match {
            case IRStoreMD(path : String, value : MetaOperator,_) => List(value)
            case IRReadMD(paths : List[String], loader,_) => List()
            case IRCollapseMD(_,value)  => List(value)
            case IRSelectMD(metaCondition, inputDataset : MetaOperator) => List(inputDataset)
            case IRPurgeMD(regionDataset : RegionOperator, inputDataset : MetaOperator) => List(inputDataset, regionDataset)
            case IRSemiJoin(externalMeta : MetaOperator, joinCondition : MetaJoinCondition, inputDataset : MetaOperator) => List(externalMeta, inputDataset)
            case IRProjectMD(projectedAttributes : Option[List[String]], metaAggregator : Option[MetaAggregateStruct], inputDataset : MetaOperator) =>List(inputDataset)
            case IRUnionMD(leftDataset: MetaOperator, rightDataset: MetaOperator, leftName : String, rightName : String) => List(leftDataset, rightDataset)
            case IRAggregateRD(aggregator : List[RegionsToMeta], inputDataset : RegionOperator) => List(inputDataset)
            case IRCombineMD(grouping : Option[MetaJoinOperator], leftDataset : MetaOperator, rightDataset : MetaOperator, leftName : String, rightName : String) => List(leftDataset, rightDataset) ++ { if(grouping.isDefined) List(grouping.get) else List()}
            case IRMergeMD(dataset : MetaOperator, groups : Option[MetaGroupOperator]) => List(dataset) ++ { if(groups.isDefined) List(groups.get) else List()}
            case IROrderMD(ordering : List[(String,Direction)], newAttribute : String, topParameter : TopParameter, inputDataset : MetaOperator) => List(inputDataset)
            case IRGroupMD(keys : MetaGroupByCondition, aggregates : List[RegionsToMeta], groupName : String, inputDataset : MetaOperator, region_dataset : RegionOperator) => List(inputDataset, region_dataset)
            case IRStoreRD(path : String, value : RegionOperator,_,_,_) => List(value)
            case IRReadRD(paths : List[String], loader,_) => List()
            case IRSelectRD(regionCondition : Option[RegionCondition], filteredMeta : Option[MetaOperator], inputDataset : RegionOperator) => List(inputDataset) ++ { if(filteredMeta.isDefined) List(filteredMeta.get) else List()}
            case IRPurgeRD(metaDataset : MetaOperator, inputDataset : RegionOperator) => List(metaDataset, inputDataset)
            case IRProjectRD(projectedValues : Option[List[Int]], tupleAggregator : Option[RegionExtension], inputDataset : RegionOperator) => List(inputDataset)
            case IRGenometricMap(grouping : Option[MetaJoinOperator], aggregators : List[RegionAggregate.RegionsToRegion], reference : RegionOperator, experiments : RegionOperator) => List(reference, experiments) ++ { if(grouping.isDefined) List(grouping.get) else List()}
            case IRRegionCover(coverFlag : CoverFlag, min : CoverParam, max : CoverParam, aggregators : List[RegionsToRegion], grouping : Option[MetaGroupOperator], inputDataset : RegionOperator) => List(inputDataset) ++ { if(grouping.isDefined) List(grouping.get) else List()}
            case IRUnionRD(schemaReformatting : List[Int], leftDataset : RegionOperator, rightDataset : RegionOperator) => List(leftDataset, rightDataset)
            case IRMergeRD(dataset : RegionOperator, groups : Option[MetaGroupOperator]) => List(dataset) ++ { if(groups.isDefined) List(groups.get) else List()}
            case IRGroupRD(groupingParameters : Option[List[GroupRDParameters.GroupingParameter]], aggregates : Option[List[RegionAggregate.RegionsToRegion]], regionDataset : RegionOperator) => List(regionDataset)
            case IROrderRD(ordering : List[(Int, Direction)], topPar : TopParameter, inputDataset : RegionOperator) => List(inputDataset)
            case IRGenometricJoin(metajoinCondition : Option[MetaJoinOperator], joinCondition : List[JoinQuadruple], regionBuilder : RegionBuilder, leftDataset : RegionOperator, rightDataset : RegionOperator) => List(leftDataset, rightDataset) ++ { if(metajoinCondition.isDefined) List(metajoinCondition.get) else List()}
            case IRDifferenceRD(metaJoin : Option[MetaJoinOperator], leftDataset : RegionOperator, rightDataset : RegionOperator, _) => List(leftDataset, rightDataset) ++ { if(metaJoin.isDefined) List(metaJoin.get) else List()}
            case IRJoinBy(condition :  MetaJoinCondition, leftDataset : MetaOperator, rightDataset : MetaOperator) => List(leftDataset, rightDataset)
            case IRGroupBy(groupAttributes: MetaGroupByCondition, inputDataset: MetaOperator) => List(inputDataset)
          }
        }

        val childrenExt : HashMap[IROperator, (Int, Set[IROperator])] =
          numAssigner(children, number, Some(list.head), new HashMap[IROperator, (Int, Set[IROperator])])

        enumerateNodesHelper( {list.tail ++ children} , {number + children.size} , {acc.merged(childrenExt){case ((a,b1),(_,b2)) => (a, (b2._1, b1._2 ++ b2._2))}} )
      }
    }

    val startingList : List[IROperator] =
      graph
        .flatMap((v) => {
        List(v.metaDag, v.regionDag)
      })

    val startingAcc = numAssigner(startingList, 0, None, new HashMap[IROperator, (Int, Set[IROperator])])

    enumerateNodesHelper(startingList, startingList.size, startingAcc).toList.sortBy((v) => v._2._1).map((v) => (v._1, v._2._2)).reverse

  }
}
