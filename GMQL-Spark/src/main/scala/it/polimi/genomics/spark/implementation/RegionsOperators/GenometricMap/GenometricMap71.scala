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
        BINNING_PARAMETER

    execute(executor, grouping, aggregator, ref, exp, binningParameter, REF_PARALLILISM, sc)
  }

  @throws[SelectFormatException]
  def execute(executor: GMQLSparkExecutor, grouping: OptionalMetaJoinOperator, aggregator: List[RegionAggregate.RegionsToRegion], ref: RDD[GRECORD], exp: RDD[GRECORD], BINNING_PARAMETER: Long, REF_PARALLILISM: Int, sc: SparkContext): RDD[GRECORD] = {
    val groups = executor.implement_mjd(grouping, sc).flatMap { x => x._2.map(s => (x._1, s)) }

    val refGroups: Broadcast[collection.Map[Long, Iterable[Long]]] = sc.broadcast(groups.groupByKey().collectAsMap())


    implicit val orderGRECORD: Ordering[(GRecordKey, Array[GValue])] = Ordering.by { ar: GRECORD => ar._1 }

    val expBinned = exp.binDS(BINNING_PARAMETER, aggregator)
    val refBinnedRep = ref.repartition(200).binDS(BINNING_PARAMETER, refGroups)


    val RefExpJoined: RDD[(Long, (GRecordKey, Array[GValue], Array[GValue], (Int, Array[Int])))] =
      refBinnedRep
        .cogroup(expBinned)
        .flatMap {
          case (key: (Long, String, Int), (ref: Iterable[(Long, Long, Long, Char, Array[GValue])], exp: Iterable[(Long, Long, Char, Array[GValue])])) =>
            // key: (Long, String, Int) sampleId, chr, bin
            // ref: Iterable[(10, Long, Long, Char, Array[GValue])] sampleId, start, stop, strand, others
            // exp: Iterable[(Long, Long, Char, Array[GValue])] start, stop, strand, others
            ref.flatMap { refRecord =>
              val newID = Hashing.md5().newHasher().putLong(refRecord._1).putLong(key._1).hash().asLong
              //val newID = key._1
              //val aggregation:Long = newID+refRecord._2+refRecord._3+refRecord._5.mkString("/").hashCode
              val aggregation = Hashing.md5().newHasher()
                .putLong(newID)
                .putString(key._2, java.nio.charset.Charset.defaultCharset())
                .putLong(refRecord._2)
                .putLong(refRecord._3)
                .putChar(refRecord._4)
                .putString(refRecord._5.mkString("/"), java.nio.charset.Charset.defaultCharset())
                .hash().asLong()

              val expTemp = exp.flatMap { expRecord =>
                if ( /* space overlapping */
                  (refRecord._2 < expRecord._2 && expRecord._1 < refRecord._3)
                    && /* same strand */
                    (refRecord._4.equals('*') || expRecord._3.equals('*') || refRecord._4.equals(expRecord._3))
                    && /* first comparison (start bin of either the ref or exp)*/
                    ((refRecord._2 / BINNING_PARAMETER).toInt.equals(key._3) || (expRecord._1 / BINNING_PARAMETER).toInt.equals(key._3))
                )
                  Some((aggregation, (new GRecordKey(newID, key._2, refRecord._2, refRecord._3, refRecord._4), refRecord._5, expRecord._4, (1, expRecord._4.map(s => if (s.isInstanceOf[GNull]) 0 else 1).iterator.toArray))))
                else
                  None
              }

              if (expTemp.isEmpty)
                Some((aggregation, (new GRecordKey(newID, key._2, refRecord._2, refRecord._3, refRecord._4), refRecord._5, Array[GValue](), (0, Array(0)))))
              else
                expTemp
            }

        }


    //_3: exp others
    val reduced = RefExpJoined.reduceByKey { (l: (GRecordKey, Array[GValue], Array[GValue], (Int, Array[Int])), r) =>
      val values: Array[GValue] =
        if (l._3.nonEmpty && r._3.nonEmpty) {
          aggregator.zipWithIndex.map { case (a, i) =>
            a.fun(List(l._3(i), r._3(i)))
          }.toArray
        } else if (r._3.nonEmpty)
          r._3
        else
          l._3

      (l._1, l._2, values, (l._4._1 + r._4._1, l._4._2.zip(r._4._2).map(s => s._1 + s._2).iterator.toArray))
    } //cache()

    val output = reduced.map { res: (Long, (GRecordKey, Array[GValue], Array[GValue], (Int, Array[Int]))) =>
      val newVal: Array[GValue] = aggregator.zipWithIndex.map { case (f, i) =>
        val valList =
          if (res._2._3.nonEmpty)
            res._2._3(i)
          else
            GDouble(0.0000000000000001)

        f.funOut(valList, (res._2._4._1, if (res._2._3.nonEmpty) res._2._4._2(i) else 0))
      }.toArray
      (res._2._1, (res._2._2 :+ GDouble(res._2._4._1)) ++ newVal)
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