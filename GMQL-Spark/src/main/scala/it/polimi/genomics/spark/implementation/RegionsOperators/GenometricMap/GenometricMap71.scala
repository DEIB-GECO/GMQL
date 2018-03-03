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

import scala.collection.mutable.ArrayBuffer
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
        BINNING_PARAMETER

    execute(executor, grouping, aggregator, ref, exp, binningParameter, REF_PARALLILISM, sc)
  }

  case class MapKey(/*sampleId: Long, */ newId: Long, refChr: String, refStart: Long, refStop: Long, refStrand: Char, refValues: List[GValue]) {

    override def equals(obj: scala.Any): Boolean = {
      if (## == obj.##) {
        val that = obj.asInstanceOf[MapKey]
        // val result = this.productIterator.zip(that.productIterator).map(x=>x._1 equals x._2).reduce(_ && _)
        this.refStart == that.refStart &&
          this.refStop == that.refStop &&
          this.refStrand == that.refStrand &&
          //  this.sampleId == that.sampleId &&
          this.newId == that.newId &&
          this.refChr == that.refChr &&
          this.refValues == that.refValues
      }
      else
        false
    }

    @transient override lazy val hashCode: Int = MurmurHash3.productHash(this)
  }

  //possible solution is generate GRecordKey and add the others as list

  @throws[SelectFormatException]
  def execute(executor: GMQLSparkExecutor, grouping: OptionalMetaJoinOperator, aggregator: List[RegionAggregate.RegionsToRegion], ref: RDD[GRECORD], exp: RDD[GRECORD], BINNING_PARAMETER: Long, REF_PARALLILISM: Int, sc: SparkContext): RDD[GRECORD] = {
    val groups = executor.implement_mjd(grouping, sc).flatMap { x => x._2.map(s => (x._1, s)) }

    val refGroups: Broadcast[collection.Map[Long, Iterable[Long]]] = sc.broadcast(groups.groupByKey().collectAsMap())


    implicit val orderGRECORD: Ordering[(GRecordKey, Array[GValue])] = Ordering.by { ar: GRECORD => ar._1 }

    val expBinned = exp.binDS(BINNING_PARAMETER, aggregator)
    val refBinnedRep = ref.repartition(sc.defaultParallelism * 32 - 1).binDS(BINNING_PARAMETER, refGroups)

    val emptyArrayGValue = Array.empty[GValue]
    val emptyArrayInt = Array.empty[Int]


    val indexedAggregator = aggregator.zipWithIndex

    val reduceFunc: ((Array[GValue], Int, Array[Int]), (Array[GValue], Int, Array[Int])) => (Array[GValue], Int, Array[Int]) = {
      case ((leftValues: Array[GValue], leftCount: Int, leftCounts: Array[Int]), (rightValues: Array[GValue], rightCount: Int, rightCounts: Array[Int])) =>
        val values: Array[GValue] =
          if (leftValues.nonEmpty && rightValues.nonEmpty) {
            indexedAggregator.map { case (a, i) =>
              a.fun(List(leftValues(i), rightValues(i)))
            }.toArray
          } else if (rightValues.nonEmpty)
            rightValues
          else
            leftValues

        (values, leftCount + rightCount, leftCounts.zipAll(rightCounts, 0, 0).map(s => s._1 + s._2))
    }

    val RefExpJoined: RDD[(MapKey, (Array[GValue], Int, Array[Int]))] =
      refBinnedRep
        .cogroup(expBinned)
        .flatMap {
          case (key: (Long, String, Int), (ref: Iterable[(Long, Long, Long, Char, Array[GValue])], exp: Iterable[(Long, Long, Char, Array[GValue])])) =>
            // key: (Long, String, Int) sampleId, chr, bin
            // ref: Iterable[(Long, Long, Long, Char, Array[GValue])] newSampleId, start, stop, strand, others
            // exp: Iterable[(Long, Long, Char, Array[GValue])] start, stop, strand, others

            ref
              .iterator
              .map { refRecord =>
                val mapKey = MapKey(/*key._1,*/ refRecord._1, key._2, refRecord._2, refRecord._3, refRecord._4, refRecord._5.toList)

                val refInStartBin = (refRecord._2 / BINNING_PARAMETER).toInt.equals(key._3)
                val isRefStrandBoth = refRecord._4.equals('*')

                val expFiltered = exp
                  .iterator
                  .filter(expRecord =>
                    (/*space overlapping*/
                      refRecord._2 < expRecord._2 && expRecord._1 < refRecord._3) &&
                      /* same strand */
                      (isRefStrandBoth || expRecord._3.equals('*') || refRecord._4.equals(expRecord._3)) &&
                      /* first comparison (start bin of either the ref or exp)*/
                      (refInStartBin || (expRecord._1 / BINNING_PARAMETER).toInt.equals(key._3))
                  )

                if (expFiltered.nonEmpty) { //if there is a match ref against exp
                  val expReduced: (Array[GValue], Int, Array[Int]) = expFiltered
                    .map(expRecord => (expRecord._4, 1, expRecord._4.map(s => if (s.isInstanceOf[GNull]) 0 else 1)))
                    .reduce(reduceFunc)
                  (mapKey, expReduced)
                }
                else if (refInStartBin) //if there is not match and in order to add one time ref we check if ref starts here
                  (mapKey, (emptyArrayGValue, 0, emptyArrayInt))
                else
                  (mapKey, (emptyArrayGValue, -1, emptyArrayInt))

              }.filter(_._2._2 != -1)
        }

    val reduced = RefExpJoined.reduceByKey(reduceFunc)

    val output: RDD[(GRecordKey, Array[GValue])] = reduced.map { case (mapKey, (values, count, counts)) =>
      val newVal = indexedAggregator.map { case (f, i) =>
        val value: GValue =
          if (values.nonEmpty)
            values(i)
          else
            GNull()
        f.funOut(value, (count, if (counts.nonEmpty) counts(i) else 0))
      }

      val newID = mapKey.newId //Hashing.md5().newHasher().putLong(mapKey.refId).putLong(mapKey.sampleId).hash().asLong

      val gRecordKey = GRecordKey(newID, mapKey.refChr, mapKey.refStart, mapKey.refStop, mapKey.refStrand)

      //default size is 16
      val buffer = new ArrayBuffer[GValue]

      buffer ++= mapKey.refValues
      buffer += GDouble(count)
      buffer ++= newVal

      (gRecordKey, buffer.toArray)
    }

    output
  }

  implicit class Binning(rdd: RDD[GRECORD]) {
    def binDS(bin: Long, aggregator: List[RegionAggregate.RegionsToRegion]): RDD[((Long, String, Int), (Long, Long, Char, Array[GValue]))] =
      rdd.flatMap { x =>
        //        if (bin > 0) {
        val startBin = (x._1._3 / bin).toInt
        val stopBin = (x._1._4 / bin).toInt

        val newVal: Array[GValue] = aggregator
          .map((f: RegionAggregate.RegionsToRegion) => {
            x._2(f.index)
          }).toArray

        (startBin to stopBin).map(i => ((x._1._1, x._1._2, i), (x._1._3, x._1._4, x._1._5, newVal)))
      }

    def binDS(bin: Long, Bgroups: Broadcast[collection.Map[Long, Iterable[Long]]]): RDD[((Long, String, Int), (Long, Long, Long, Char, Array[GValue]))] =
      rdd.flatMap { x =>
        val startBin = (x._1._3 / bin).toInt
        val stopBin = (x._1._4 / bin).toInt


        Bgroups.value.getOrElse(x._1._1, Iterable[Long]()).flatMap { exp_id =>
          val newID = Hashing.md5().newHasher().putLong(x._1._1).putLong(exp_id).hash().asLong

          (startBin to stopBin).map { i =>
            ((exp_id, x._1._2, i), (newID, x._1._3, x._1._4, x._1._5, x._2))
          }
        }

      }


  }

}