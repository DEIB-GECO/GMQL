package it.polimi.genomics.flink.FlinkImplementation.operator.region

import it.polimi.genomics.core.DataStructures.{MetaGroupOperator, RegionOperator}
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.flink.FlinkImplementation.FlinkImplementation
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

/**
 * Created by michelebertoni on 04/06/15.
 */
object MergeRD {

  final val logger = LoggerFactory.getLogger(this.getClass)
  @throws[SelectFormatException]
  def apply(executor : FlinkImplementation, dataset : RegionOperator, groups : Option[MetaGroupOperator], env : ExecutionEnvironment) : DataSet[FlinkRegionType] = {

    val ds : DataSet[FlinkRegionType] =
      executor.implement_rd(dataset, env)

    val groupedDs : DataSet[FlinkRegionType] =
      if (groups.isDefined) {
        //group
        val grouping = executor.implement_mgd(groups.get, env);
        assignGroups(ds, grouping)
      } else {
        //union of samples
        ds.map((r) => {
          (1L, r._2, r._3, r._4, r._5, r._6)
        })
      }

    groupedDs


    /*
    val distinctGroupedDs: DataSet[FlinkRegionType] =
      groupedDs
        //create a list of each element in the array
        .map((r) => {
          (r._1, r._2, r._3, r._4, r._5, r._6.foldLeft(new Array[List[GValue]](0))((z, g : GValue) => z :+ List(g)))
        })
        //group by coordinates
        .groupBy(0,1,2,3,4)
        //reduce coincident regions
        .reduce((a, b) => {
          (a._1, a._2, a._3, a._4, a._5,
            a._6
              .zip(b._6)
              .map((c) => c._1 ++ c._2))
        })
        //apply aggregate functions
        .map((r) => {
          (r._1, r._2, r._3, r._4, r._5,
            new Array[GValue](0) ++
            aggregator
              .map((f: RegionAggregate.RegionAggregateMap) => {
              f.fun(r._6(f.index))
            })
          )
        })


        distinctGroupedDs
    */
  }


  def assignGroups(dataset : DataSet[FlinkRegionType], grouping : DataSet[FlinkMetaGroupType2]) : DataSet[FlinkRegionType] = {
    dataset.joinWithTiny(grouping).where(0).equalTo(0){
      (r : FlinkRegionType, g : (Long, Long), out : Collector[FlinkRegionType]) => {
        out.collect((g._2, r._2, r._3, r._4, r._5, r._6))
      }
    }
  }


}
