package it.polimi.genomics.flink.FlinkImplementation.operator.region

import it.polimi.genomics.core.DataStructures.GroupMDParameters.Direction.Direction
import it.polimi.genomics.core.DataStructures.GroupMDParameters._
import it.polimi.genomics.core.DataStructures.RegionOperator
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.{GString, GDouble, GValue}
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.flink.FlinkImplementation.FlinkImplementation
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._
import org.apache.flink.hadoop.shaded.com.google.common.base.Charsets
import org.apache.flink.hadoop.shaded.com.google.common.hash.Hashing
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

/**
 * Created by michelebertoni on 16/06/15.
 */
object OrderRD {

  final val logger = LoggerFactory.getLogger(this.getClass)
  @throws[SelectFormatException]
  def apply(executor : FlinkImplementation, ordering : List[(Int, Direction)], topParameter : TopParameter, inputDataset : RegionOperator, env : ExecutionEnvironment) : DataSet[FlinkRegionType] = {
    val ds : DataSet[(Long, String, Long, Long, Char, Array[GValue])] =
      executor.implement_rd(inputDataset, env)

    val grouping : Boolean =
      topParameter match {
        case NoTop() => false
        case Top(_) => false
        case TopG(_) => true
      }

    val top : Int =
      topParameter match {
        case NoTop() => 0
        case Top(v) => v
        case TopG(v) => v
      }

    val sampleGValueType : Array[GValue] =
      ds.first(1).collect().head._6

    //extract the indexes that will be used in grouping/ordering
    val keys : List[Int] =
      ordering.map(_._1)

    val sortedGroupsOfRegions : GroupedDataSet[FlinkRegionType] =
      if(grouping){
        sort(
          ds.groupBy((r : FlinkRegionType) => {
            val s = new StringBuilder
            s.append(r._1)
            keys.init.foreach((i) => s.append("§").append(r._6(i)))
            Hashing.md5().hashString(s.toString(), Charsets.UTF_8).asLong()
          }), List(ordering.last), sampleGValueType)
      } else {
        sort(ds.groupBy((r : FlinkRegionType) => r._1), ordering, sampleGValueType)
      }

    val sortedToppedRegions : DataSet[FlinkRegionType] =
      sortedGroupsOfRegions
        .reduceGroup((i : Iterator[FlinkRegionType], out : Collector[FlinkRegionType]) => {
          assignPosition(i, 1, top, out)
        })

    sortedToppedRegions

  }

  def assignPosition(i : Iterator[FlinkRegionType], position : Double, top : Int, out : Collector[FlinkRegionType]) : Unit = {
    if(i.hasNext && (top == 0 || top >= position)){
      val c = i.next()
      out.collect((c._1, c._2, c._3, c._4, c._5, c._6 :+ GDouble(position)))
      assignPosition(i, position+1, top, out)
    }
  }

  def sort(ds : GroupedDataSet[FlinkRegionType], ordering : List[(Int, Direction)], sampleType : Array[GValue]) : GroupedDataSet[(Long, String, Long, Long, Char, Array[GValue])] = {
    if(ordering.size > 0) {
      sampleType(ordering.head._1) match {
        case GString(v) => subSortString(ds, ordering, sampleType)
        case GDouble(v) => subSortDouble(ds, ordering, sampleType)
      }
    } else {
      ds
    }
  }


  def subSortString(ds : GroupedDataSet[FlinkRegionType], ordering : List[(Int, Direction)], sampleType : Array[GValue]) : GroupedDataSet[(Long, String, Long, Long, Char, Array[GValue])] = {
    val step : GroupedDataSet[(Long, String, Long, Long, Char, Array[GValue])] =
      ds
        .sortGroup(((r: FlinkRegionType) => {
          r._6(ordering.head._1).asInstanceOf[GString].v
        }
          ),
          ordering.head._2 match {
            case Direction.ASC => Order.ASCENDING
            case Direction.DESC => Order.DESCENDING
          }
        )

    sort(step, ordering.drop(1), sampleType)

  }


  def subSortDouble(ds : GroupedDataSet[FlinkRegionType], ordering : List[(Int, Direction)], sampleType : Array[GValue]) : GroupedDataSet[(Long, String, Long, Long, Char, Array[GValue])] = {
    val step : GroupedDataSet[(Long, String, Long, Long, Char, Array[GValue])] =
      ds
        .sortGroup(((r: FlinkRegionType) => {
        r._6(ordering.head._1).asInstanceOf[GDouble].v
      }
        ),
          ordering.head._2 match {
            case Direction.ASC => Order.ASCENDING
            case Direction.DESC => Order.DESCENDING
          }
        )

    sort(step, ordering.drop(1), sampleType)

  }

}
