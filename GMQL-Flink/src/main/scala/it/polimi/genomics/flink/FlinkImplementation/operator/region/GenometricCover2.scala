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
object GenometricCover2 {
  final val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Binning size: this is used also in GMapForCover
   */
  //final val BINNING_PARAMETER : Long = 50000L

  @throws[SelectFormatException]
  def apply(executor : FlinkImplementation, coverFlag : CoverFlag, min : CoverParam, max : CoverParam, aggregators : List[RegionsToRegion], grouping : Option[MetaGroupOperator], inputDataset : RegionOperator, binSize : Long, env : ExecutionEnvironment) : DataSet[FlinkRegionType] = {
    // CREATE DATASET RECURSIVELY
    val ds: DataSet[FlinkRegionType] =
      executor.implement_rd(inputDataset, env)

    val groups = if(grouping.isDefined){
      Some(executor.implement_mgd(grouping.get, env))
    } else {
      None
    }

    val groupIds = if(groups.isDefined){
      groups.get.distinct(1).map(_._2).collect().toList
    } else {
      List(0L)
    }


    val groupedDs : DataSet[(Long, String, Long, Long, Char, Long, Array[GValue])] =
      assignGroups(executor, ds, groups, env)


    // EXTRACT START-STOP POINTS
    val extracted : DataSet[(Long, String, Char, Int, HashMap[Int, Int])] =
      extractor(groupedDs, binSize)


    // PREPARE COVER PARAMETERS
    // calculate the value only if needed
    val allValue : HashMap[Long, Int] =
      if(min.isInstanceOf[ALL] || max.isInstanceOf[ALL]){
        groupedDs
          .map((v) => (v._1, v._6, 1))
          .distinct
          .groupBy(0)
          .reduce((a,b) =>
            (a._1, a._2, a._3+b._3)
          )
          .map((v) =>
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
        case ANY() => groupIds.map(v => ((v, 1))).foldRight(new HashMap[Long, Int])((a,z) => z + a) //new HashMap[Long,Int] + ((0L,1))
        case N(value) => groupIds.map(v => ((v, value))).foldRight(new HashMap[Long, Int])((a,z) => z + a)  //new HashMap[Long,Int] + ((0L,value))
      }

    val maximum : HashMap[Long, Int] =
      max match{
        case ALL() => allValue
        case ANY() => groupIds.map(v => ((v, Int.MaxValue))).foldRight(new HashMap[Long, Int])((a,z) => z + a) //new HashMap[Long, Int] + ((0L,Int.MaxValue))
        case N(value) => groupIds.map(v => ((v, value))).foldRight(new HashMap[Long, Int])((a,z) => z + a) //new HashMap[Long,Int] + ((0L,value))
      }

    // EXECUTE COVER ON BINS
    val ss =
      extracted
        // collapse coincident point
        .groupBy(0,1,2,3)
        .reduce((a,b) => {
          (a._1, a._2, a._3, a._4, a._5.merged(b._5)({case ((k,v1),(_,v2)) => (k,v1+v2)}))
        }).withForwardedFields("0;1;2;3")

    val binnedPureCover : DataSet[FlinkRegionType] =ss.flatMap((bin : (Long, String, Char, Int, HashMap[Int, Int]), out : Collector[FlinkRegionType]) => {
          val points : List[(Int, Int)] = bin._5.toList.sortBy(_._1)
          coverFlag match {
            case CoverFlag.COVER => coverHelper(0L, 0, 0, false, minimum, maximum, points.iterator, out, bin._1, bin._2, bin._3, bin._4, binSize)
            case CoverFlag.HISTOGRAM => histogramHelper(0L, 0, minimum, maximum, points.iterator, out, bin._1, bin._2, bin._3, bin._4, binSize)
            case CoverFlag.SUMMIT => summitHelper(0L, 0, false, false, minimum, maximum, false, points.iterator, out, bin._1, bin._2, bin._3, bin._4, binSize)
            case CoverFlag.FLAT => coverHelper(0L, 0, 0, false, minimum, maximum, points.iterator, out, bin._1, bin._2, bin._3, bin._4, binSize)
          }
        }).withForwardedFields("0;1;2->4")

//    //SPLIT DATASET -> FIND REGIONS THAT MAY CROSS A BIN
//    val valid: DataSet[FlinkRegionType] =
//      binnedPureCover.filter((v) => (v._3 % binSize != 0 && v._4 % binSize != 0))
//    val notValid: DataSet[FlinkRegionType] =
//      binnedPureCover.filter((v) => (v._3 % binSize == 0 || v._4 % binSize == 0))
//
//
//    // JOIN REGIONS THAT
//    // ARE IN THE SAME CHROMOSOME
//    // HAVE COINCIDENT STOP AND START
//    // HAVE SAME INTERSECTION COUNTER
//    val joined : DataSet[FlinkRegionType] =
//      notValid
//        .groupBy(0,1,4)
//        .sortGroup(2, Order.ASCENDING)
//        .reduceGroup((i : Iterator[FlinkRegionType], out : Collector[FlinkRegionType]) => {
//          if(i.hasNext) {
//            var old: FlinkRegionType =
//              i.next()
//
//            while (i.hasNext) {
//              val current = i.next()
//              if (old._4.equals(current._3) && old._6(0).asInstanceOf[GDouble].v.equals(current._6(0).asInstanceOf[GDouble].v)){
//                old = (old._1, old._2, old._3, current._4, old._5, old._6)
//              } else {
//                out.collect(old)
//                old = current
//              }
//            }
//            out.collect(old)
//          }
//        })//.withForwardedFields("0;1;2;4;5")

    val pureCover : DataSet[FlinkRegionType] = binnedPureCover
//      joined.union(valid)
//    println ("Flink, Joined: "+joined.count)
      //valid
//    println("Flink, Total: "+pureCover.count())
    // USE GENOMETRIC MAP TO CALCULATE AGGREGATION
    // calculate aggregation
    val flat : Boolean =
      coverFlag.equals(CoverFlag.FLAT)

//    println("Flat: "+ flat)
    val aggregated : DataSet[FlinkRegionType] = GMapForCover3(aggregators, flat, pureCover, groupedDs, binSize)
      //GMapForCover3(aggregators, flat, pureCover, groupedDs, binSize)

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
   * @param id
   * @param chr
   * @param strand
   * @param bin
   * @param binSize
   */
  @tailrec
  final def coverHelper(start : Long, count : Int, countMax : Long, recording : Boolean, minimum : HashMap[Long,Int], maximum : HashMap[Long,Int], i : Iterator[(Int, Int)], out : Collector[(FlinkRegionType)], id : Long, chr : String, strand : Char, bin : Int, binSize : Long) : Unit = {
    if(i.hasNext) {
      val current = i.next

      val newCount =
        count + current._2

      val newRecording =
        (newCount >= minimum.get(id).getOrElse(1) && newCount <= maximum.get(id).getOrElse(Int.MaxValue) && i.hasNext)

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
        if (newCount >= minimum.get(id).getOrElse(1) && newCount <= maximum.get(id).getOrElse(Int.MaxValue) && !recording) {
          val begin = 0
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
        val end = binSize
        out.collect((id, chr, newStart + bin * binSize, if (current._1 > end) end + bin * binSize else current._1 + bin * binSize, strand, new Array[GValue](0) :+ GDouble(countMax)))
      }
      coverHelper(newStart, newCount, newCountMax, newRecording, minimum, maximum, i, out, id, chr, strand, bin, binSize)
    }
  }

  /**
   * Tail recursive helper for histogram
   *
   * @param start
   * @param count
   * @param minimum
   * @param maximum
   * @param i
   * @param out
   * @param id
   * @param chr
   * @param strand
   * @param bin
   * @param binSize
   */
  @tailrec
  final def histogramHelper(start : Long, count : Int, minimum : HashMap[Long,Int], maximum : HashMap[Long,Int], i : Iterator[(Int, Int)], out : Collector[(FlinkRegionType)], id : Long, chr : String, strand : Char, bin : Int, binSize : Long) : Unit = {
    if(i.hasNext) {
      val current = i.next
      val newCount =
        count + current._2
      if (start.equals(current._1)) {
        histogramHelper(start, newCount, minimum, maximum, i, out, id, chr, strand, bin, binSize)
      } else {
        val newStart: Long =
          if (newCount >= minimum.get(id).getOrElse(1) && newCount <= maximum.get(id).getOrElse(Int.MaxValue) && newCount != count) {
            val begin = 0
            if (current._1 < begin) {
              begin
            }
            else {
              current._1
            }
          } else {
            start
          }
        //print("" + start + " " + newStart + " " + count + " " + newCount + " ")
        if (count >= minimum.get(id).getOrElse(1) && count <= maximum.get(id).getOrElse(Int.MaxValue) && newCount != count && count != 0) {
          //output a region
          val end = binSize
          //print(if (current._1 > end) end + bin * BINNING_PARAMETER else current._1 + bin * BINNING_PARAMETER + " print")
          out.collect((id, chr, start + bin * binSize, if (current._1 > end) end + bin * binSize else current._1 + bin * binSize, strand, new Array[GValue](0) :+ GDouble(count)))
        }
        //println()
        histogramHelper(newStart, newCount, minimum, maximum, i, out, id, chr, strand, bin, binSize)
      }
    }
  }


  /**
   * Tail recursive helper for summit
   *
   * @param start
   * @param count
   * @param valid
   * @param minimum
   * @param maximum
   * @param growing
   * @param i
   * @param out
   * @param id
   * @param chr
   * @param strand
   * @param bin
   * @param binSize
   */
  @tailrec
  final def summitHelper(start : Long, count : Int, valid : Boolean, reEnteredInValidZoneDecreasing : Boolean, minimum : HashMap[Long,Int], maximum : HashMap[Long,Int], growing : Boolean, i : Iterator[(Int, Int)], out : Collector[(FlinkRegionType)], id : Long, chr : String, strand : Char, bin : Int, binSize : Long) : Unit = {
    //TODO TOTALLY CHANGED
    /*
    if(i.hasNext) {
      val current =
        i.next

      val newCount : Int =
        count + current._2

      val newGrowing : Boolean =
        newCount > count || (growing && newCount.equals(count))

      val newStart : Long =
        if (newCount != count) {
          val begin = 0
          if (current._1 < begin) {
            begin
          }
          else {
            current._1
          }
        } else {
          start
        }

      if (count >= minimum.get(id).getOrElse(0) && count <= maximum.get(id).getOrElse(0) && newCount != count && /*count != 0 &&*/ growing && !newGrowing) {
        //output a region
        val end = binSize
        out.collect((id, chr, start, if (current._1 > end) end + bin * binSize else current._1 + bin * binSize, strand, new Array[GValue](0) :+ GDouble(count)))
      }

      summitHelper(newStart, newCount, minimum, maximum, newGrowing, i, out, id, chr, strand, bin, binSize)
    }
    */

    if(i.hasNext) {
      val current : (Int, Int) =
        i.next

      val newCount : Int =
        count + current._2

      val newValid : Boolean =
        newCount >= minimum.get(id).getOrElse(1) && newCount <= maximum.get(id).getOrElse(Int.MaxValue)

      val newGrowing : Boolean =
        newCount > count || (growing && newCount.equals(count))

      val newReEnteredInValidZoneDecreasing : Boolean =
        !valid && newValid

      if (start.equals(current._1)) {
        summitHelper(start, newCount, newValid, reEnteredInValidZoneDecreasing, minimum, maximum, growing, i, out, id, chr, strand, bin, binSize)
      } else {
        val newStart: Long =
          if (newValid && newCount != count) {
            val begin = 0
            if (current._1 < begin) {
              begin
            }
            else {
              current._1
            }
          } else {
            start
          }

        //println(current._1 + " " + valid + " " + newValid + " " + growing + " " + newGrowing + " " + count + " " + newCount)

        //print("" + start + " " + newStart + " " + count + " " + newCount + " ")
        if ( ( (valid && growing && (!newGrowing || !newValid ) ) ||   (reEnteredInValidZoneDecreasing && !newGrowing)) && count != 0) {
          //println(current._1)
          //output a region
          val end = binSize
          //print(if (current._1 > end) end + bin * BINNING_PARAMETER else current._1 + bin * BINNING_PARAMETER + " print")
          out.collect((id, chr, start + bin * binSize, if (current._1 > end) end + bin * binSize else current._1 + bin * binSize, strand, new Array[GValue](0) :+ GDouble(count)))
        }
        //println()
        summitHelper(newStart, newCount, newValid, newReEnteredInValidZoneDecreasing, minimum, maximum, newGrowing, i, out, id, chr, strand, bin, binSize)
      }
    }
  }


  //PREPARATORS

  def assignGroups(executor : FlinkImplementation, dataset : DataSet[FlinkRegionType], grouping : Option[DataSet[FlinkMetaGroupType2]], env : ExecutionEnvironment): DataSet[(Long, String, Long, Long, Char, Long, Array[GValue])] = {
    if(grouping.isDefined){
      val groups = grouping.get

      dataset.join(groups).where(0).equalTo(0) {
        (r, g, out: Collector[(Long, String, Long, Long, Char, Long, Array[GValue])]) =>
          if (r._5.equals('*')) {
            out.collect((g._2, r._2, r._3, r._4, '-', r._1, r._6))
            out.collect((g._2, r._2, r._3, r._4, '+', r._1, r._6))
          } else {
            out.collect((g._2, r._2, r._3, r._4, r._5, r._1, r._6))
          }
      }.withForwardedFieldsFirst("1;2;3;0->5").withForwardedFieldsSecond("1->0")

    } else {
      dataset.flatMap((r, out : Collector[(Long, String, Long, Long, Char, Long, Array[GValue])]) => {
        if (r._5.equals('*')) {
          out.collect((0L, r._2, r._3, r._4, '-', r._1, r._6))
          out.collect((0L, r._2, r._3, r._4, '+', r._1, r._6))
        } else {
          out.collect((0L, r._2, r._3, r._4, r._5, r._1, r._6))
        }
      })//.withForwardedFields("1;2;3;0->5;5->6")
    }
  }

  def extractor(dataset : DataSet[(Long, String, Long, Long, Char, Long, Array[GValue])], binSize : Long) : DataSet[(Long, String, Char, Int, HashMap[Int, Int])] = {
    dataset.flatMap((x) => {
      val startBin = (x._3/binSize).toInt
      val stopBin = (x._4/binSize).toInt

      if(startBin==stopBin) {
        List((x._1, x._2, x._5, startBin, HashMap((x._3 - startBin * binSize).toInt -> 1, (x._4 - startBin * binSize).toInt -> -1)))
      }
      else{
        val map_start = (x._1, x._2, x._5, startBin, HashMap((x._3 - startBin * binSize).toInt -> 1, binSize.toInt -> -1))
        val map_stop = (x._1, x._2, x._5, stopBin, HashMap(0 -> 1, (x._4 - stopBin * binSize).toInt -> -1))
        val map_int = for (i <- startBin+1 to stopBin-1) yield (x._1, x._2, x._5, i, HashMap(0->1, binSize.toInt -> -1))

        List(map_start, map_stop) ++ map_int
      }
    })
  }




}
