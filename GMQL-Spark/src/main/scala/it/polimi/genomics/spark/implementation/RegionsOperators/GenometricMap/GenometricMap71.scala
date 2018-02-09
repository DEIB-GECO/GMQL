package it.polimi.genomics.spark.implementation.RegionsOperators.GenometricMap

import com.google.common.hash.Hashing
import it.polimi.genomics.core.DataStructures._
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.core.{GDouble, GNull, GRecordKey, GValue}
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.util.hashing.MurmurHash3


/**
  * Created by abdulrahman kaitoua on 08/08/15.
  * The main bottle neck is in line 191, takes hours to repliocate the reference for every experiment
  * same as version 7 but with join on the ids for the reference and the regions and extra partitioner.
  */
object GenometricMap71 {
  private final val logger = LoggerFactory.getLogger(this.getClass)
  private final type groupType = Array[((Long, String), Array[Long])]

  @throws[SelectFormatException]
  def apply(executor: GMQLSparkExecutor, grouping: OptionalMetaJoinOperator, aggregator: List[RegionAggregate.RegionsToRegion], reference: RegionOperator, experiments: RegionOperator, BINNING_PARAMETER: Long, REF_PARALLILISM: Int, sc: SparkContext): RDD[GRECORD] = {
    logger.info("----------------MAP71 executing -------------")
    //creating the datasets
    val ref: RDD[(GRecordKey, Array[GValue])] =
      executor.implement_rd(reference, sc)
    val exp: RDD[(GRecordKey, Array[GValue])] =
      executor.implement_rd(experiments, sc)

    val binningParameter =
      if (BINNING_PARAMETER == 0)
        Long.MaxValue
      else
        180 * 1000

    execute(executor, grouping, aggregator, ref, exp, binningParameter, REF_PARALLILISM, sc)
  }

  case class MapKey(sampleId: Long, refId: Long, refChr: String, refStart: Long, refStop: Long, refStrand: Char, refValues: List[GValue]) {

    override def equals(obj: scala.Any): Boolean = {
      if (## == obj.##) {
        val that = obj.asInstanceOf[MapKey]
        // val result = this.productIterator.zip(that.productIterator).map(x=>x._1 equals x._2).reduce(_ && _)
        this.refStart == that.refStart &&
          this.refStop == that.refStop &&
          this.refStrand == that.refStrand &&
          this.sampleId == that.sampleId &&
          this.refId == that.refId &&
          this.refChr == that.refChr &&
          this.refValues == that.refValues
      }
      else
        false
    }

    @transient override lazy val hashCode: Int = MurmurHash3.productHash(this)
  }

  @throws[SelectFormatException]
  def execute(executor: GMQLSparkExecutor, grouping: OptionalMetaJoinOperator, aggregator: List[RegionAggregate.RegionsToRegion], ref: RDD[GRECORD], exp: RDD[GRECORD], BINNING_PARAMETER: Long, REF_PARALLILISM: Int, sc: SparkContext): RDD[GRECORD] = {
    val groups = executor.implement_mjd(grouping, sc).flatMap { x => x._2.map(s => (x._1, s)) }

    val refGroups: Broadcast[collection.Map[Long, Iterable[Long]]] = sc.broadcast(groups.groupByKey().collectAsMap())


    implicit val orderGRECORD: Ordering[(GRecordKey, Array[GValue])] = Ordering.by { ar: GRECORD => ar._1 }

    val expBinned = exp.binDS(BINNING_PARAMETER, aggregator)
    val refBinnedRep = ref.binDS(BINNING_PARAMETER, refGroups)


    val RefExpJoined =
      refBinnedRep
        .cogroup(expBinned)
        .flatMap {
          case (key: (Long, String, Int), (ref: Iterable[(Long, Long, Long, Char, Array[GValue])], exp: Iterable[(Long, Long, Char, Array[GValue])])) =>
            // key: (Long, String, Int) sampleId, chr, bin
            // ref: Iterable[(10, Long, Long, Char, Array[GValue])] sampleId, start, stop, strand, others
            // exp: Iterable[(Long, Long, Char, Array[GValue])] start, stop, strand, others
            ref.flatMap { refRecord =>
              lazy val mapKey = MapKey(key._1, refRecord._1, key._2, refRecord._2, refRecord._3, refRecord._4, refRecord._5.toList)

              val refInStartBin = (refRecord._2 / BINNING_PARAMETER).toInt.equals(key._3)
              val isRefStrandBoth = refRecord._4.equals('*')
              val expTemp = exp.flatMap { expRecord =>
                if (
                /* space overlapping */
                  (refRecord._2 < expRecord._2 && expRecord._1 < refRecord._3)
                    && /* same strand */
                    (isRefStrandBoth || expRecord._3.equals('*') || refRecord._4.equals(expRecord._3))
                    && /* first comparison (start bin of either the ref or exp)*/
                    (refInStartBin || (expRecord._1 / BINNING_PARAMETER).toInt.equals(key._3))
                )
                //if there is a overlapping on the same strand and one of the regions(ref or exp) starts at this bin
                  Some((mapKey, (expRecord._4, 1, expRecord._4.map(s => if (s.isInstanceOf[GNull]) 0 else 1))))
                else
                  None
              }

              if (expTemp.nonEmpty) //if there is a match ref against exp
                expTemp
              else if (refInStartBin) //if there is not match and in order to add one time ref we check if ref starts here
                Some((mapKey, (Array.empty[GValue], 0, Array.empty[Int])))
              else
                None
            }
        }

    val reduced = RefExpJoined.reduceByKey { case ((leftValues, leftCount, leftCounts), (rightValues, rightCount, rightCounts)) =>
      val values: Array[GValue] =
        if (leftValues.nonEmpty && rightValues.nonEmpty) {
          aggregator.zipWithIndex.map { case (a, i) =>
            a.fun(List(leftValues(i), rightValues(i)))
          }.toArray
        } else if (rightValues.nonEmpty)
          rightValues
        else
          leftValues

      (values, leftCount + rightCount, leftCounts.zip(rightCounts).map(s => s._1 + s._2))
    }

    val output = reduced.map { case (mapKey, (values, count, counts)) =>
      val newVal = aggregator.zipWithIndex.map { case (f, i) =>
        val value: GValue =
          if (values.nonEmpty)
            values(i)
          else
            GNull()
        f.funOut(value, (count, if (counts.nonEmpty) counts(i) else 0))
      }

      val newID = Hashing.md5().newHasher().putLong(mapKey.refId).putLong(mapKey.sampleId).hash().asLong

      val gRecordKey = GRecordKey(newID, mapKey.refChr, mapKey.refStart, mapKey.refStop, mapKey.refStrand)

      (gRecordKey, ((mapKey.refValues :+ GDouble(count)) ++ newVal).toArray)
    }

    output
  }

  implicit class Binning(rdd: RDD[GRECORD]) {
    def binDS(bin: Long, aggregator: List[RegionAggregate.RegionsToRegion]): RDD[((Long, String, Int), (Long, Long, Char, Array[GValue]))] =
      rdd.flatMap { x =>
        //        if (bin > 0) {
        val startbin = (x._1._3 / bin).toInt
        val stopbin = (x._1._4 / bin).toInt
        val newVal: Array[GValue] = aggregator
          .map((f: RegionAggregate.RegionsToRegion) => {
            x._2(f.index)
          }).toArray
        //          println (newVal.mkString("/"))
        for (i <- startbin to stopbin)
          yield ((x._1._1, x._1._2, i), (x._1._3, x._1._4, x._1._5, newVal))
        //        } else
        //          {
        //            val newVal: Array[GValue] = aggregator
        //              .map((f: RegionAggregate.RegionsToRegion) => {
        //                x._2(f.index)
        //              }).toArray
        //            //          println (newVal.mkString("/"))
        //              Some((x._1._1, x._1._2, 0), (x._1._3, x._1._4, x._1._5, newVal))
        //          }
      }

    def binDS(bin: Long, Bgroups: Broadcast[collection.Map[Long, Iterable[Long]]]): RDD[((Long, String, Int), (Long, Long, Long, Char, Array[GValue]))] =
    //        rdd.keyBy(x=>x._1._1).join(Bgroups).flatMap { x =>
    //        if (bin > 0) {
    //            val startbin = (x._2._1._1._3 / bin).toInt
    //            val stopbin = (x._2._1._1._4 / bin).toInt
    //              (startbin to stopbin).map(i =>
    //                ((x._2._2, x._2._1._1._2, i), (x._2._1._1._1, x._2._1._1._3, x._2._1._1._4, x._2._1._1._5, x._2._1._2))
    //              )
    //        }else
    //             Some((x._2._2, x._2._1._1._2, 0), (x._2._1._1._1, x._2._1._1._3, x._2._1._1._4, x._2._1._1._5, x._2._1._2))
    //      }
      rdd.flatMap { x =>
        val startbin = (x._1._3 / bin).toInt
        val stopbin = (x._1._4 / bin).toInt

        (startbin to stopbin).flatMap { i =>
          Bgroups.value.getOrElse(x._1._1, Iterable[Long]())
            .map(exp_id =>
              ((exp_id, x._1._2, i), (x._1._1, x._1._3, x._1._4, x._1._5, x._2))
            )
        }
      }


  }

}