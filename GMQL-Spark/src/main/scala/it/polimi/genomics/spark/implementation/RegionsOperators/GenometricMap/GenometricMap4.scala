package it.polimi.genomics.spark.implementation.RegionsOperators.GenometricMap

import com.google.common.base.Charsets
import com.google.common.hash.Hashing
import it.polimi.genomics.core.DataStructures._
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.GRecordKey
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import it.polimi.genomics.core.{GValue, GDouble}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.collection.Map


/**
  * Created by abdulrahman kaitoua on 13/05/15.
  */
object GenometricMap4 {

  //private final val BINNING_PARAMETER = 50000

  final val logger = LoggerFactory.getLogger(this.getClass)

  @throws[SelectFormatException]
  def apply(executor: GMQLSparkExecutor, grouping: OptionalMetaJoinOperator, aggregator: List[RegionAggregate.RegionsToRegion], reference: RegionOperator, experiments: RegionOperator, binSize: Long, sc: SparkContext): RDD[GRECORD] = {

    //creating the datasets
    val ref: RDD[GRECORD] = executor.implement_rd(reference, sc)
    val exp: RDD[GRECORD] = executor.implement_rd(experiments, sc)

    //    println("\n\n----------------- ref count = " + ref.count() + "\n\n")
    //    println("\n\n----------------- exp count = " + exp.count() + "\n\n")

    execute(executor, grouping, aggregator, ref, exp, binSize, sc)
  }

  @throws[SelectFormatException]
  def execute(executor: GMQLSparkExecutor, grouping: OptionalMetaJoinOperator, aggregator: List[RegionAggregate.RegionsToRegion], ref: RDD[GRECORD], exp: RDD[GRECORD], binSize: Long, sc: SparkContext): RDD[GRECORD] = {
    //creating the groups if they exist
    //otherwise create all possible (ref,exp) pairs
    val pairs: RDD[SparkMetaJoinType] =
    //      if(grouping.isInstanceOf[SomeMetaJoinOperator]){
      executor.implement_mjd(grouping, sc)
    //      } else {
    //        ref.map(_._1._1).distinct.cartesian(exp.map(_._1._1).distinct()).groupByKey().mapValues(x=>x.toArray)
    //      }
    val Bgroups = Some(sc.broadcast(pairs.collectAsMap()))

    //pairs.collect().foreach(println _)
    //    println("\n\n----------------- group count = " + pairs.count() + "\n\n")
    //(sampleID, sampleID)

    //group the datasets
    //(expid,Chr,bin)(refID, start, Stop, str, Values)
    val groupedRef: RDD[((Long, String, Int), (Long, Long, Long, Char, Array[GValue], Long))]
    /*: DataSet[(Long, Long, Int, Int, Long, String, Long, Long, Char, Array[GValue], Long)]*/ =
      assignRegionGroups(ref, Bgroups, binSize)

    //(expid,Chr,bin)(start, Stop, str, Values)
    val groupedExp: RDD[((Long, String, Int), (Long, Long, Char, Array[GValue]))]
    /*: DataSet[(Int, Int, Long, String, Long, Long, Char, Array[GValue])]*/ =
      assignExperimentGroups(exp, binSize, aggregator)
    //ref(hashId, expId, binStart, bin, refid, chr, start, stop, strand, GValues, aggregationID)
    //exp(binStart, bin, expId, chr, start, stop., strand, GValues)

    //prepare a neutral extension of the array that matches the aggregator types
    //neutral means GInt(0), GDouble(0,0) and GString("")

    val extraData: List[Array[List[GValue]]] =
    //      List(groupedExp.first._2._4.map(_ => List[GValue]()))
      List(aggregator.map(_ => List[GValue]()).toArray)
    //aggregator.map(_ => List[GValue]()).toArray
    //createSampleArrayExtension(aggregator, exp.first(1).collect.map(_._6).headOption.getOrElse(new Array[GValue](0)))

    //Join phase
    val coGroupResult: RDD[(Long, (Long, String, Long, Long, Char, Array[GValue], Array[List[GValue]], Int))] =
      groupedRef.cogroup(groupedExp).flatMap { x => val references = x._2._1;
        val experiments = x._2._2
        val refCollected: List[PartialResult] = references.map(r => (new PartialResult(r, 0, extraData))).toList
        for (r <- refCollected) {
          for (e <- experiments) {
            if ( /* space overlapping */
              (r.binnedRegion._2 < e._2 && e._1 < r.binnedRegion._3)
                && /* same strand */
                (r.binnedRegion._4.equals('*') || e._3.equals('*') || r.binnedRegion._4.equals(e._3))
                && /* first comparison (start bin of either the ref or exp)*/
                ((r.binnedRegion._2 / binSize).toInt.equals(x._1._3) || (e._1 / binSize).toInt.equals(x._1._3))
            ) {
              r.count += 1
              r.extra = r.extra :+ e._4.foldLeft(Array[List[GValue]]())((z: Array[List[GValue]], v: GValue) => z :+ List(v))
            }
          }
        }
        refCollected.map{pr =>
          val hashID = Hashing.md5().newHasher().putLong(pr.binnedRegion._1).putLong(x._1._1).hash().asLong;
          (pr.binnedRegion._6, (hashID, x._1._2, pr.binnedRegion._2, pr.binnedRegion._3, pr.binnedRegion._4, pr.binnedRegion._5, pr.extra.reduce((a, b) => a.zip(b).map((p) => p._1 ++ p._2)), pr.count))
        }
      }

    //Aggregation phase
    val aggregationResult/*: DataSet[(Long, String, Long, Long, Char, Array[GValue])]*/ =
      coGroupResult
        .reduceByKey{(r1, r2) =>
                val out: (Long, String, Long, Long, Char, Array[GValue], Array[List[GValue]], Int) = (r1._1, r1._2, r1._3, r1._4, r1._5, r1._6,
                  r1._7
                    .zip(r2._7)
                    .map((a) => a._1 ++ a._2),
                  r1._8 + r2._8)
                out

              }
        //apply aggregation function on extra data
        .map{l=>var i=0
        val newVal:Array[GValue] = aggregator.map{f=>val valList = if(l._2._7.size >0)l._2._7(i) else {List[GValue]()}; val out = f.fun(valList);i = i+1; out}.toArray
        val out = (new GRecordKey(l._2._1, l._2._2,l._2._3, l._2._4, l._2._5), l._2._6 ++ (GDouble(l._2._8) +: newVal))

        out
      }
    //OUTPU
    //    println("aggregation "+aggregationResult.count)
    aggregationResult
  }




  def assignRegionGroups(rdd: RDD[GRECORD], Bgroups: Option[Broadcast[Map[Long, Array[Long]]]], binSize: Long): RDD[((Long, String, Int), (Long, Long, Long, Char, Array[GValue], Long))] = {
    rdd.flatMap { x =>


      if (Bgroups.isDefined) {
        val startbin = (x._1._3 / binSize).toInt
        val stopbin = (x._1._4 / binSize).toInt
        val group = Bgroups.get.value.get(x._1._1)
        if (group.isDefined)
          (startbin to stopbin).flatMap(i =>
            group.get.map { id => val aggID = Hashing.md5().newHasher().putString("" + x._1._1 + id + x._1._2 + x._1._3 + x._1._4 + x._1._5 + x._2.mkString("/"), Charsets.UTF_8).hash().asLong
              (((id, x._1._2, i), (x._1._1, x._1._3, x._1._4, x._1._5, x._2, aggID)))
            }
          )
        else None
      }
      else None
    }
  }

  def assignExperimentGroups(rdd: RDD[GRECORD], binSize: Long, aggregator: List[RegionAggregate.RegionsToRegion]): RDD[((Long, String, Int), (Long, Long, Char, Array[GValue]))] =
    rdd.flatMap { x =>
      val startbin = (x._1._3 / binSize).toInt
      val stopbin = (x._1._4 / binSize).toInt
      val newVal: Array[GValue] = aggregator
        .flatMap((f: RegionAggregate.RegionsToRegion) => {
          List(x._2(f.index))
        }).toArray
      //          println (newVal.mkString("/"))
      for (i <- startbin to stopbin)
        yield ((x._1._1, x._1._2, i), (x._1._3, x._1._4, x._1._5, newVal))
    }

  class PartialResult(val binnedRegion: (Long, Long, Long, Char, Array[GValue], Long), var count: Int, var extra: List[Array[List[GValue]]]) {
    override def toString(): String = {
      "PR" + binnedRegion.toString() + " --" + count + "-- " + extra.mkString("-")
    }
  }

}