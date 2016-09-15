package it.polimi.genomics.flink.FlinkImplementation.operator.region

import it.polimi.genomics.core.DataStructures.GroupRDParameters.{FIELD, GroupingParameter}
import it.polimi.genomics.core.DataStructures.{GroupRDParameters, RegionAggregate, RegionOperator}
import it.polimi.genomics.core.DataTypes.FlinkRegionType
import it.polimi.genomics.core.GValue
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.flink.FlinkImplementation.FlinkImplementation
import org.apache.flink.api.scala._
import org.apache.flink.hadoop.shaded.com.google.common.base.Charsets
import org.apache.flink.hadoop.shaded.com.google.common.hash.Hashing
import org.slf4j.LoggerFactory

/**
 * Created by michelebertoni on 14/06/15.
 */
object GroupRD {

  final val logger = LoggerFactory.getLogger(this.getClass)
  @throws[SelectFormatException]
  def apply(executor : FlinkImplementation, groupingParameters : Option[List[GroupRDParameters.GroupingParameter]], aggregates : Option[List[RegionAggregate.RegionsToRegion]], regionDataset : RegionOperator, env : ExecutionEnvironment) : DataSet[FlinkRegionType] = {
    val ds = executor.implement_rd(regionDataset, env)

    val res : DataSet[(Long, String, Long, Long, Char, Array[GValue])] =
      ds
        //data preparation
        .map((r : FlinkRegionType) => {
          (r._1, r._2, r._3, r._4, r._5, r._6.map((g : GValue) => List(g)))
        })
        //grouping
        .groupBy((r : (Long, String, Long, Long, Char, Array[List[GValue]])) => {
        val s = new StringBuilder
        s.setLength(0)
          s.append(r._1.toString)
          s.append(r._2.toString)
          s.append(r._3.toString)
          s.append(r._4.toString)
          s.append(r._5.toString)
          if(groupingParameters.isDefined){
            groupingParameters.get.foreach((g : GroupingParameter) => {
              g match {
                case FIELD(pos) => s.append("§").append(r._6(pos).mkString("§"))
              }
            })
          }
          Hashing.md5().hashString(s.toString(), Charsets.UTF_8).asLong()
        })
        //aggregating
        .reduce((a : (Long, String, Long, Long, Char, Array[List[GValue]]), b : (Long, String, Long, Long, Char, Array[List[GValue]])) => {
          (a._1, a._2, a._3, a._4, a._5, a._6.zip(b._6).map((c) => c._1 ++ c._2))
        })
        //applying functions
        .map((a : (Long, String, Long, Long, Char, Array[List[GValue]])) => {
          val aggregated : Array[GValue] =
            if(aggregates.isDefined){
              aggregates.get.foldLeft(new Array[GValue](0))((z, agg) => z :+ agg.fun(a._6(agg.index)))
            } else {
              new Array[GValue](0)
            }

          (a._1, a._2, a._3, a._4, a._5, aggregated)
        })
    res
  }

}
