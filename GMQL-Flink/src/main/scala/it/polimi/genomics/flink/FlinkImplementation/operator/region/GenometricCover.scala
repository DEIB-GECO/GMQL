package it.polimi.genomics.flink.FlinkImplementation.operator.region

import it.polimi.genomics.core.DataStructures.CoverParameters.CoverFlag.CoverFlag
import it.polimi.genomics.core.DataStructures.CoverParameters._
import it.polimi.genomics.core.DataStructures.RegionAggregate.RegionsToRegion
import it.polimi.genomics.core.DataStructures.{MetaGroupOperator, RegionOperator}
import it.polimi.genomics.core.DataTypes.{FlinkMetaGroupType2, FlinkRegionType}
import it.polimi.genomics.core.{GDouble, GValue}
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.flink.FlinkImplementation.FlinkImplementation
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.collection.immutable.HashMap

/**
 * Created by michelebertoni on 24/05/15.
 */
object GenometricCover {
  final val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Binning size: this is used also in GMapForCover
   */
  final val BINNING_PARAMETER = 50000

  @throws[SelectFormatException]
  def apply(executor : FlinkImplementation, coverFlag : CoverFlag, min : CoverParam, max : CoverParam, aggregators : List[RegionsToRegion], grouping : Option[MetaGroupOperator], inputDataset : RegionOperator, env : ExecutionEnvironment) : DataSet[FlinkRegionType] = {
    // CREATE DATASET RECURSIVELY
    val ds: DataSet[FlinkRegionType] =
      executor.implement_rd(inputDataset, env)

    val groupedDs: DataSet[(Long, Int, Long, String, Long, Long, Char, Int, Array[GValue])] =
      assignGroups(executor, ds, grouping, env)

    // EXTRACT START-STOP POINTS
    val extracted: DataSet[(Long, Long, Char, String, Int, Int)] =
      extractor(groupedDs)

    // PREPARE COVER PARAMETERS
    // calculate the value only if needed
    val allValue : HashMap[Long, Int] =
      if(min.isInstanceOf[ALL] || max.isInstanceOf[ALL]){
        groupedDs
          .map((v) => (v._1, v._3, 1))
          .distinct
          .groupBy(0)
          .reduce((a,b) =>
            (a._1, a._2, a._3+b._3)
          )
          .map(v =>
            (v._1, v._3)
          )
          .collect()
          .foldLeft(new HashMap[Long, Int])((h,v) => h + v)
      } else {
        new HashMap[Long, Int] + ((0L,0))
      }

    val minimum : HashMap[Long, Int] =
      min match{
        case ALL() => allValue
        case ANY() => new HashMap[Long, Int] + ((0L,1))
        case N(value) => new HashMap[Long,Int] + ((0L,value))
      }

    val maximum : HashMap[Long, Int] =
      max match{
        case ALL() => allValue
        case ANY() => new HashMap[Long, Int] + ((0L,Int.MaxValue))
        case N(value) => new HashMap[Long,Int] + ((0L,value))
      }

    // EXECUTE COVER ON BINS
    val binnedPureCover : DataSet[FlinkRegionType] =
      extracted
        // collapse coincident point
        .groupBy(0,1,2,3,5)
        .reduce((a,b) => {
          (a._1, a._2, a._3, a._4, a._5+b._5, a._6)
        })
        // cover
        .groupBy(1,2,3,5)
        .sortGroup(0, Order.ASCENDING)
        .reduceGroup((i : Iterator[(Long, Long, Char, String, Int, Int)], out : Collector[(FlinkRegionType)]) => {
          coverFlag match {
            case CoverFlag.COVER => coverHelper(0L, 0, 0, false, minimum, maximum, i, out)
            case CoverFlag.HISTOGRAM => histogramHelper(0L, 0, minimum, maximum, i, out)
            case CoverFlag.SUMMIT => summitHelper(0L, 0, minimum, maximum, false, i, out)
            case CoverFlag.FLAT => coverHelper(0L, 0, 0, false, minimum, maximum, i, out)
          }
        })


    //SPLIT DATASET -> FIND REGIONS THAT MAY CROSS A BIN
    val valid: DataSet[FlinkRegionType] =
      binnedPureCover.filter((v) => (v._3 % BINNING_PARAMETER != 0 && v._4 % BINNING_PARAMETER != 0))
    val notValid: DataSet[FlinkRegionType] =
      binnedPureCover.filter((v) => (v._3 % BINNING_PARAMETER == 0 || v._4 % BINNING_PARAMETER == 0))

    // JOIN REGIONS THAT
    // ARE IN THE SAME CHROMOSOME
    // HAVE COINCIDENT STOP AND START
    // HAVE SAME INTERSECTION COUNTER
    val joined : DataSet[FlinkRegionType] =
      notValid
        .groupBy(1)
        .sortGroup(2, Order.ASCENDING)
        .reduceGroup((i : Iterator[FlinkRegionType], out : Collector[FlinkRegionType]) => {
          if(i.hasNext) {
            var old: FlinkRegionType =
              i.next()

            while (i.hasNext) {
              val current = i.next()
              if (old._4.equals(current._3) && old._6(0).asInstanceOf[GDouble].v.equals(current._6(0).asInstanceOf[GDouble].v)){
                old = (old._1, old._2, old._3, current._4, old._5, old._6)
              } else {
                out.collect(old)
                old = current
              }
            }
            out.collect(old)
          }
        })

    val pureCover : DataSet[FlinkRegionType] =
      joined.union(valid)

    // USE GENOMETRIC MAP TO CALCULATE AGGREGATION
    // calculate aggregation
    val flat : Boolean =
      coverFlag.equals(CoverFlag.FLAT)

    val aggregated : DataSet[FlinkRegionType] =
      GMapForCover(aggregators, flat, pureCover, groupedDs)

    aggregated

  }

  // EXECUTORS

  /**
   * Tail recursive helper for cover
   *
   * @param start
   * @param count
   * @param countMax
   * @param recording
   * @param minimum
   * @param maximum
   * @param i
   * @param out
   */
  @tailrec
  final def coverHelper(start : Long, count : Int, countMax : Long, recording : Boolean, minimum : HashMap[Long,Int], maximum : HashMap[Long,Int], i : Iterator[(Long, Long, Char, String, Int, Int)], out : Collector[(FlinkRegionType)]) : Unit = {
    if(i.hasNext) {
      val current = i.next
      val newCount =
        count + current._5
      val newRecording =
        (newCount >= minimum.get(current._2).getOrElse(0) && newCount <= maximum.get(current._2).getOrElse(0) && i.hasNext)
      val newCountMax =
        if (!newRecording && recording) {
          0
        } else {
          if (newRecording && newCount > countMax) {
            newCount
          } else {
            countMax
          }
        }
      val newStart: Long =
        if (newCount >= minimum.get(current._2).getOrElse(0) && newCount <= maximum.get(current._2).getOrElse(0) && !recording) {
          val begin = (current._6) * BINNING_PARAMETER
          if (current._1 < begin) {
            begin
          }
          else {
            current._1
          }
        } else {
          start
        }
      if (!newRecording && recording) {
        //output a region
        val end = ((current._6 + 1) * BINNING_PARAMETER)
        out.collect((current._2, current._4, newStart, if (current._1 > end) end else current._1, current._3, new Array[GValue](0) :+ GDouble(countMax)))
      }
      coverHelper(newStart, newCount, newCountMax, newRecording, minimum, maximum, i, out)
    }
  }

  /**
   * Tail recursive helper for histogram
   *
   * @param start
   * @param count
   * @param countMax
   * @param recording
   * @param minimum
   * @param maximum
   * @param i
   * @param out
   */
  @tailrec
  final def histogramHelper(start : Long, count : Int, minimum : HashMap[Long,Int], maximum : HashMap[Long,Int], i : Iterator[(Long, Long, Char, String, Int, Int)], out : Collector[(FlinkRegionType)]) : Unit = {
    if(i.hasNext) {
      val current = i.next
      val newCount =
        count + current._5
      val newStart: Long =
        if (newCount >= minimum.get(current._2).getOrElse(0) && newCount <= maximum.get(current._2).getOrElse(0) && newCount != count) {
          val begin = (current._6) * BINNING_PARAMETER
          if (current._1 < begin) {
            begin
          }
          else {
            current._1
          }
        } else {
          start
        }
      if (newCount != count && count != 0) {
        //output a region
        val end = ((current._6 + 1) * BINNING_PARAMETER)
        out.collect((current._2, current._4, start, if (current._1 > end) end else current._1, current._3, new Array[GValue](0) :+ GDouble(count)))
      }
      histogramHelper(newStart, newCount, minimum, maximum, i, out)
    }
  }


  /**
   * Tail recursive helper for summit
   *
   * @param start
   * @param count
   * @param minimum
   * @param maximum
   * @param growing
   * @param i
   * @param out
   */
  @tailrec
  final def summitHelper(start : Long, count : Int, minimum : HashMap[Long,Int], maximum : HashMap[Long,Int], growing : Boolean, i : Iterator[(Long, Long, Char, String, Int, Int)], out : Collector[(FlinkRegionType)]) : Unit = {
    if(i.hasNext) {
      val current : (Long, Long, Char, String, Int, Int) =
        i.next

      val newCount : Int =
        count + current._5

      val newGrowing : Boolean =
        newCount > count || (growing && newCount.equals(count))

      val newStart : Long =
        if (newCount != count) {
          val begin = (current._6) * BINNING_PARAMETER
          if (current._1 < begin) {
            begin
          }
          else {
            current._1
          }
        } else {
          start
        }

      if (count >= minimum.get(current._2).getOrElse(0) && count <= maximum.get(current._2).getOrElse(0) && newCount != count && /*count != 0 &&*/ growing && !newGrowing) {
        //output a region
        val end = ((current._6 + 1) * BINNING_PARAMETER)
        out.collect((current._2, current._4, start, if (current._1 > end) end else current._1, current._3, new Array[GValue](0) :+ GDouble(count)))
      }

      summitHelper(newStart, newCount, minimum, maximum, newGrowing, i, out)
    }
  }


  //PREPARATORS

  def assignGroups(executor : FlinkImplementation, dataset : DataSet[FlinkRegionType], grouping : Option[MetaGroupOperator], env : ExecutionEnvironment): DataSet[(Long, Int, Long, String, Long, Long, Char, Int, Array[GValue])] = {
    if(grouping.isDefined){
      val groups : DataSet[FlinkMetaGroupType2] =
        executor.implement_mgd(grouping.get, env)

      dataset.join(groups).where(0).equalTo(0) {
        (r, g, out: Collector[(Long, Int, Long, String, Long, Long, Char, Int, Array[GValue])]) =>
          //g._2.map((g) => {
            val binStart = (r._3 / BINNING_PARAMETER).toInt
            val binEnd = (r._4 / BINNING_PARAMETER).toInt
            for (i <- binStart to binEnd) {
              if (r._5.equals('*')) {
                out.collect((g._2, i, r._1, r._2, r._3, r._4, '-', binStart, r._6))
                out.collect((g._2, i, r._1, r._2, r._3, r._4, '+', binStart, r._6))
              } else {
                out.collect((g._2, i, r._1, r._2, r._3, r._4, r._5, binStart, r._6))
              }
            }
        //})
      }

    } else {
      dataset.flatMap((r, out : Collector[(Long, Int, Long, String, Long, Long, Char, Int, Array[GValue])]) => {
        val binStart = (r._3 / BINNING_PARAMETER).toInt
        val binEnd = (r._4 / BINNING_PARAMETER).toInt
        for (i <- binStart to binEnd) {
          if(r._5.equals('*')){
            out.collect((0L, i, r._1, r._2, r._3, r._4, '-', binStart, r._6))
            out.collect((0L, i, r._1, r._2, r._3, r._4, '+', binStart, r._6))
          } else {
            out.collect((0L, i, r._1, r._2, r._3, r._4, r._5, binStart, r._6))
          }
        }
      })
    }
  }

  def extractor(dataset : DataSet[(Long, Int, Long, String, Long, Long, Char, Int, Array[GValue])]) = {
    dataset.flatMap((r, out : Collector[(Long, Long, Char, String, Int, Int)]) => {
      out.collect((r._5, r._1, r._7, r._4, 1, r._2))
      out.collect((r._6, r._1, r._7, r._4, -1, r._2))
    })
  }




}
