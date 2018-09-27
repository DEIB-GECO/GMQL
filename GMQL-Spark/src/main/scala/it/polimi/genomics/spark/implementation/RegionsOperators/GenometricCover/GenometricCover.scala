package it.polimi.genomics.spark.implementation.RegionsOperators.GenometricCover

import it.polimi.genomics.core.DataStructures.CoverParameters.CoverFlag.CoverFlag
import it.polimi.genomics.core.DataStructures.CoverParameters._
import it.polimi.genomics.core.DataStructures.RegionAggregate.RegionsToRegion
import it.polimi.genomics.core.DataStructures.{MetaGroupOperator, RegionOperator}
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.{GDouble, GValue}
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import it.polimi.genomics.spark.implementation.RegionsOperators.GenometricMap.GMAP4
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.collection.Map
import scala.collection.immutable.HashMap
import scala.xml.dtd.ANY

/**
  * Created by Abdulrahman Kaitoua on 10/08/15.
  */
object GenometricCover {
  type Grecord = (Long, String, Long, Long, Char, Array[GValue])

  private final val logger = LoggerFactory.getLogger(GenometricCover.getClass);

  @throws[SelectFormatException]
  def apply(executor: GMQLSparkExecutor, coverFlag: CoverFlag, min: CoverParam, max: CoverParam, aggregators: List[RegionsToRegion], grouping: Option[MetaGroupOperator], inputDataset: RegionOperator, binSize: Long, sc: SparkContext): RDD[GRECORD] = {
    logger.info("----------------Cover executing..")

    // CREATE DATASET RECURSIVELY
    val ds: RDD[GRECORD] = executor.implement_rd(inputDataset, sc)

    val groups: Option[Broadcast[Map[Long, Long]]] = if (grouping.isDefined)
      Some(sc.broadcast(executor.implement_mgd(grouping.get, sc).collectAsMap()))
    else None

    val groupIds = if (groups.isDefined) {
      groups.get.value.toList.map(_._2).distinct
    } else {
      List(0L)
    }

    //ASSGIN GROUPS TO THE DATA AND IF NOT GROUPED GIVE ID 0 AS THE GROUP ID
    val groupedDs: RDD[(Long, String, Long, Long, Char, Long, Array[GValue])] =
      assignGroups(ds, groups)

    // EXTRACT START-STOP POINTS
    val extracted: RDD[((String, Int, Long, Char), HashMap[Int, Int])] =
      extractor(groupedDs, binSize)


    // PREPARE COVER PARAMETERS
    // calculate the value only if needed
    val allValue: Map[Long, Int] =
    if (min.isInstanceOf[ALL] || max.isInstanceOf[ALL]) {
      if (groups.isDefined) {
        // SWITCH (sampleID, GroupID) to (GroupID,SampleID) THEN GROUP BY GROUPID AND COUNT THE SAMPLES IN EACH GROUP
        groups.get.value.groupBy(_._2).map(x => (x._1, x._2.size))

      } else {
        val samplesCount = groupedDs.map(x => x._6).distinct().count.toInt
        groupIds.map(v => ((v, samplesCount))).foldRight(new HashMap[Long, Int])((a, z) => z + a)
      }
    } else {
      Map(0L -> 0)
    }

    val minimum: Map[Long, Int] =
      if (min.isInstanceOf[ALL]) allValue.map(x => (x._1, min.fun(x._2)))
      else if (min.isInstanceOf[ANY]) groupIds.map(v => ((v, 1))).foldRight(new HashMap[Long, Int])((a, z) => z + a)
      else groupIds.map(v => (v, min.asInstanceOf[N].n)).foldRight(new HashMap[Long, Int])((a, z) => z + a)

    val maximum: Map[Long, Int] =
      if (max.isInstanceOf[ALL]) allValue.map(x => (x._1, max.fun(x._2)))
      else if (max.isInstanceOf[ANY]) groupIds.map(v => ((v, Int.MaxValue))).foldRight(new HashMap[Long, Int])((a, z) => z + a)
      else groupIds.map(v => ((v, max.asInstanceOf[N].n))).foldRight(new HashMap[Long, Int])((a, z) => z + a)


    //    val minimum : Map[Long, Int] =
    //      min match{
    //        case ALL() => allValue
    //        case ANY() => groupIds.map(v => ((v, 1))).foldRight(new HashMap[Long, Int])((a,z) => z + a)
    //        case N(value) => groupIds.map(v => (v, value)).foldRight(new HashMap[Long, Int])((a,z) => z + a)
    //      }
    //
    //    val maximum : Map[Long, Int] =
    //      max match{
    //        case ALL() => allValue
    //        case ANY() => groupIds.map(v => ((v, Int.MaxValue))).foldRight(new HashMap[Long, Int])((a,z) => z + a)
    //        case N(value) => groupIds.map(v => ((v, value))).foldRight(new HashMap[Long, Int])((a,z) => z + a)
    //      }

    // EXECUTE COVER ON BINS
    val ss = extracted
      // collapse coincident point
      .reduceByKey((a, b) => {
      (a.merged(b)({ case ((k, v1), (_, v2)) => (k, v1 + v2) }))
    })

    val binnedPureCover: RDD[Grecord] = ss.flatMap(bin => {
      val points: List[(Int, Int)] = bin._2.toList.sortBy(_._1)
      coverFlag match {
        case CoverFlag.COVER => coverHelper(points.iterator, minimum, maximum, bin._1._3, bin._1._1, bin._1._4, bin._1._2, binSize)
        case CoverFlag.HISTOGRAM => histogramHelper(points.iterator, minimum, maximum, bin._1._3, bin._1._1, bin._1._4, bin._1._2, binSize)
        case CoverFlag.SUMMIT => summitHelper(points.iterator, minimum, maximum, bin._1._3, bin._1._1, bin._1._4, bin._1._2, binSize)
        case CoverFlag.FLAT => coverHelper(points.iterator, minimum, maximum, bin._1._3, bin._1._1, bin._1._4, bin._1._2, binSize)
      }
    })

    //SPLIT DATASET -> FIND REGIONS THAT MAY CROSS A BIN
    val valid: RDD[Grecord] =
      binnedPureCover.filter((v) => (v._3 % binSize != 0 && v._4 % binSize != 0))
    val notValid: RDD[Grecord] =
      binnedPureCover.filter((v) => (v._3 % binSize == 0 || v._4 % binSize == 0))

    // JOIN REGIONS THAT
    // ARE IN THE SAME CHROMOSOME
    // HAVE COINCIDENT STOP AND START
    // HAVE SAME INTERSECTION COUNTER
    val joined: RDD[Grecord] =
    notValid
      .groupBy(x => (x._1, x._2, x._5))
      .flatMap { x =>
        val i = x._2.toList.sortBy(x => (x._3, x._4)).iterator
        var out = List[Grecord]()
        if (i.hasNext) {
          var old: Grecord = i.next()
          while (i.hasNext) {
            val current = i.next()
            if (old._4.equals(current._3) && (!coverFlag.equals(CoverFlag.HISTOGRAM) || old._6(0).asInstanceOf[GDouble].v.equals(current._6(0).asInstanceOf[GDouble].v))) {
              old = (old._1, old._2, old._3, current._4, old._5, Array[GValue]() :+ GDouble(Math.max(old._6(0).asInstanceOf[GDouble].v, current._6(0).asInstanceOf[GDouble].v)))
            } else {
              out = out :+ old
              old = current
            }
          }
          out = out :+ old
        }
        out
      }

    val pureCover: RDD[Grecord] = valid.union(joined)

    val flat: Boolean = coverFlag.equals(CoverFlag.FLAT)
    val summit: Boolean = coverFlag.equals(CoverFlag.SUMMIT)

    GMAP4(aggregators, flat, summit, pureCover, groupedDs, binSize)
  }

  // EXECUTORS

  /**
    *
    * @param start
    * @param count
    * @param countMax
    * @param recording
    * @param minimum
    * @param maximum
    * @param i
    */
  final def coverHelper(si: Iterator[(Int, Int)], minimum: Map[Long, Int], maximum: Map[Long, Int], id: Long, chr: String, strand: Char, bin: Int, binSize: Long): List[Grecord] = {
    var start = 0L
    var count = 0
    var countMax = 0
    var recording = false
    si.flatMap { current =>

      val newCount = count + current._2

      val newRecording = (newCount >= minimum.get(id).get && newCount <= maximum.get(id).get && si.hasNext)

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
        if (newCount >= minimum.get(id).get && newCount <= maximum.get(id).get && !recording) {
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
      val out = if (!newRecording && recording) {
        //output a region
        val end = binSize
        Some(((id, chr, newStart + bin * binSize, if (current._1 > end) end + bin * binSize else current._1 + bin * binSize, strand, new Array[GValue](0) :+ GDouble(countMax))))
      } else
        None

      count = newCount
      start = newStart
      countMax = newCountMax
      recording = newRecording
      out //::: coverHelper(newStart, newCount, newCountMax, newRecording, minimum, maximum, i, id, chr, strand, bin, binSize)
    }.toList
  }

  /**
    * Tail recursive helper for histogram
    *
    * @param start
    * @param count
    * @param minimum
    * @param maximum
    * @param i
    */
  final def histogramHelper(i: Iterator[(Int, Int)], minimum: Map[Long, Int], maximum: Map[Long, Int], id: Long, chr: String, strand: Char, bin: Int, binSize: Long): List[Grecord] = {
    var count = 0
    var start = 0L
    i.flatMap { current =>
      val newCount = count + current._2
      //condition just to check the start of the bin
      if (start.equals(current._1)) {
        count = newCount
        None
      } // ALL THE ITERATIONS GOES HERE
      else {
        //IF THE NEW COUNT IS  ACCEPTABLE AND DIFFERENT FROM THE PREVIOUS COUNT = > NEW REGION START
        val newStart: Long = if (newCount >= minimum.get(id).get && newCount <= maximum.get(id).get && newCount != count)
          current._1
        else
          start

        //output a region
        val out = if (count >= minimum.get(id).get && count <= maximum.get(id).get && newCount != count && count != 0) {
          val end = binSize
          Some((id, chr, start + bin * binSize, current._1 + bin * binSize, strand, Array[GValue](GDouble(count))))
        }
        else None

        start = newStart
        count = newCount
        out
      }
    }.toList
  }

  /**
    *
    * @param start
    * @param count
    * @param minimum
    * @param maximum
    * @param growing
    * @param i
    */
  final def summitHelper(i: Iterator[(Int, Int)], minimum: Map[Long, Int], maximum: Map[Long, Int], id: Long, chr: String, strand: Char, bin: Int, binSize: Long): List[Grecord] = {
    var start = 0L
    var count = 0
    var valid = false
    var reEnteredInValidZoneDecreasing = false
    var growing = false

    i.flatMap { current =>
      val newCount: Int = count + current._2
      val newValid = newCount >= minimum.get(id).getOrElse(1) && newCount <= maximum.get(id).getOrElse(Int.MaxValue)
      val newGrowing: Boolean = newCount > count || (growing && newCount.equals(count))
      val newReEnteredInValidZoneDecreasing: Boolean = !valid && newValid

      if (start.equals(current._1)) {
        count = newCount
        valid = newValid
        reEnteredInValidZoneDecreasing = newReEnteredInValidZoneDecreasing
        growing = newGrowing
        None
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

        val out = if (((valid && growing && (!newGrowing || !newValid)) || (reEnteredInValidZoneDecreasing && !newGrowing)) && count != 0) {
          //output a region
          val end = binSize
          Some((id, chr, start + bin * binSize, if (current._1 > end) end + bin * binSize else current._1 + bin * binSize, strand, Array[GValue]() :+ GDouble(count)))
        } else None

        start = newStart
        count = newCount
        valid = newValid
        reEnteredInValidZoneDecreasing = newReEnteredInValidZoneDecreasing
        growing = newGrowing
        out
      }
    }.toList
  }

  //PREPARATORS

  /**
    * Assign Group ID to the regions
    *
    * @param dataset  input dataset
    * @param grouping Group IDs
    * @return RDD with group ids set in each sample.
    */
  def assignGroups(dataset: RDD[GRECORD], grouping: Option[Broadcast[Map[Long, Long]]]): RDD[(Long, String, Long, Long, Char, Long, Array[GValue])] = {
    val containsStrandUnknown = dataset.map(x => x._1._5 == '*').fold(false)(_ || _)
    val groups =
      if (grouping.isDefined)
        grouping.get.value
      else
        Map.empty[Long, Long].withDefaultValue(0L)

    dataset.map { x =>
      val strand =
        if (containsStrandUnknown)
          '*'
        else
          x._1._5
      (groups(x._1._1), x._1._2, x._1._3, x._1._4, strand, x._1._1, x._2)
    }
  }

  def extractor(dataset: RDD[(Long, String, Long, Long, Char, Long, Array[GValue])], binSize: Long) = {
    dataset.flatMap { x =>
      val startBin = (x._3 / binSize).toInt
      val stopBin = (x._4 / binSize).toInt

      if (startBin == stopBin) {
        List(((x._2, startBin, x._1, x._5), HashMap((x._3 - startBin * binSize).toInt -> 1, (x._4 - startBin * binSize).toInt -> -1)))
      } else {
        val map_start = ((x._2, startBin, x._1, x._5), HashMap((if (x._3 - startBin * binSize != binSize) x._3 - startBin * binSize else binSize - 1).toInt -> 1, binSize.toInt -> -1))
        val map_stop = ((x._2, stopBin, x._1, x._5), HashMap((if (x._4 - stopBin * binSize != 0) x._4 - stopBin * binSize else 1).toInt -> -1, 0 -> 1))
        val map_int = for (i <- startBin + 1 to stopBin - 1) yield ((x._2, i, x._1, x._5), HashMap(0 -> 1, binSize.toInt -> -1))

        List(map_start, map_stop) ++ map_int
      }
    }
  }


}
