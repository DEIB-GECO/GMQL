package it.polimi.genomics.spark.implementation.RegionsOperators.GenometricMap

import com.google.common.hash.Hashing
import it.polimi.genomics.core.DataStructures._
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.core.{GValue, GString, GDouble}
import it.polimi.genomics.core.{DataTypes, GRecordKey}
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.Map


/**
 * Created by abdulrahman kaitoua on 14/06/15.
 */
object GenometricMap {

  private final val BINNING_PARAMETER = 0
  private final type groupType = Array[((Long, String), Array[Long])]
  @throws[SelectFormatException]
  def apply(executor : GMQLSparkExecutor, grouping : OptionalMetaJoinOperator, aggregator : List[RegionAggregate.RegionsToRegion], reference : RegionOperator, experiments : RegionOperator, sc : SparkContext) : RDD[GRECORD] = {

    //creating the datasets
    val ref: RDD[(GRecordKey, Array[GValue])] =
      executor.implement_rd(reference, sc)
    val exp: RDD[(GRecordKey, Array[GValue])] =
      executor.implement_rd(experiments, sc)

    execute(executor, grouping, aggregator, ref, exp, sc)
  }

  @throws[SelectFormatException]
  def execute(executor : GMQLSparkExecutor, grouping : OptionalMetaJoinOperator, aggregator : List[RegionAggregate.RegionsToRegion], ref: RDD[GRECORD], exp: RDD[GRECORD], sc : SparkContext) : RDD[GRECORD] = {
    //creating the groups if they exist
    //otherwise create all possible (ref,Array[exp]) pairs
    exp.cache()
    ref.cache()
    val groups: groupType =
     /* if (grouping.isInstanceOf[SomeMetaJoinOperator]) */{
        val groupRes = executor.implement_mjd(grouping, sc).collect
        val groupExpIds = groupRes.flatMap(x => x._2).distinct
        val groupRefIds = groupRes.map(x => x._1).distinct

        val expIDs = exp.map(x => (x._1._1, x._1._2)).distinct.filter(x => groupExpIds.contains(x._1)).groupBy(x => x._1).collectAsMap()
        val refIDs = ref.map(x => (x._1._1, x._1._2)).distinct.filter(x => groupRefIds.contains(x._1)).groupBy(x => x._1).collectAsMap()
        groupRes.flatMap(x => refIDs.get(x._1).get.map(r => (r, x._2)))

      } /*else {
        val expIDs = exp.map(_._1._1).distinct.collect
        ref.map(x => (x._1._1, x._1._2)).distinct.map(x => (x, expIDs)).collect
      }*/


    //    import it.polimi.genomics.spark.implementation.RegionsOperators.GenometricMap._

    val output: RDD[(GRecordKey, Array[GValue])] = /*if (ref.count <= 1000000) {
      val expGrouped = exp.binDS(BINNING_PARAMETER).groupBy(x => (x._1._1, x._1._2)).map(chromGroup => (chromGroup._1, chromGroup._2.toArray.sortBy(x => (x._1._2, x._1._3, x._1._4)).toIterable))
      val refGrouped = ref.binDS(BINNING_PARAMETER).groupBy(x => (x._1._1, x._1._2)).map(chromGroup => (chromGroup._1, chromGroup._2.toArray.sortBy(x => (x._1._2, x._1._3, x._1._4)).toIterable))
      exp.unpersist(); ref.unpersist()

      val refBroadCasted = sc.broadcast(refGrouped.collectAsMap())
      expGrouped GMap(refBroadCasted.value, 0, 0, aggregator, groups, BINNING_PARAMETER, sc)

    } else*/ {
      val grSamRef = sc.broadcast(groups.flatMap(x=>x._2.map(s=>(s,x._1._1))).toMap)
      val expGrouped = exp.binDS(BINNING_PARAMETER).groupBy{x =>val newID = Hashing.md5().newHasher().putLong(grSamRef.value.get(x._1._1).getOrElse(x._1._1).asInstanceOf[Long]).putLong(x._1._1).hash().asLong(); (newID, x._1._2)}
        .map(chromGroup => (chromGroup._1, chromGroup._2.toArray.sortBy(x => (x._1._2, x._1._3, x._1._4)).toIterable))

      val grRefSam = sc.broadcast(groups.map(x=>(x._1._1,x._2)).toMap)

      val refGrouped = ref.binDS(BINNING_PARAMETER).flatMap(x=>
        grRefSam.value.get(x._1._1).getOrElse(Array(x._1._1)).map{s=>val newID = Hashing.md5().newHasher().putLong(x._1._1).putLong(s).hash().asLong();
          (new GRecordKey(newID,x._1._2,x._1._3,x._1._4,x._1._5),x._2)
      })
        .groupBy{x => (x._1._1, x._1._2)}
        .map(chromGroup => (chromGroup._1, chromGroup._2.toArray.sortBy(x => (x._1._2, x._1._3, x._1._4)).toIterable))
      exp.unpersist(); ref.unpersist()

      import it.polimi.genomics.spark.implementation.RegionsOperators.GenometricMap.GMAP._
      val coGroupedRefExp = refGrouped.cogroup(expGrouped).cache
      coGroupedRefExp.flatMap { grouped =>
        if (!grouped._2._2.iterator.hasNext) {None} //TODO Handle empty ref
          else if(!grouped._2._2.iterator.next.iterator.hasNext) None
        else if (!grouped._2._1.iterator.hasNext) None
          else if (!grouped._2._1.iterator.next.iterator.hasNext) None
        else GMAP.scanMapChroms(grouped._2._1.iterator.next.iterator, grouped._2._2.iterator.next.iterator, aggregator, 0, 0, BINNING_PARAMETER)
      }
    }
    output

    //    }else None
  }
  implicit class Binning(rdd: RDD[GRECORD]) {
    def binDS(bin: Int): RDD[GRECORD] =
      if (bin > 0)
        rdd.flatMap { x =>

          val startbin = Math.ceil((x._1._3 + 1) / bin).toInt
          val stopbin = Math.ceil((x._1._4 + 1) / bin).toInt
          var res = List[GRECORD]()
          var chrom = x._1._2
          if (x._1._2.indexOf('_') > 0) {
            chrom = x._1._2.split('_').mkString("")
          }
          if (startbin == stopbin) {
            res ::=(new GRecordKey(x._1._1, (chrom + "_" + startbin), x._1._3, x._1._4, x._1._5), x._2)
            res
          }
          else {
            var i = startbin
            for (i <- startbin to stopbin)
              res ::=(new GRecordKey(x._1._1, (chrom + "_" + i), x._1._3, x._1._4, x._1._5), x._2)
            res
          }
        }
      else
        rdd
  }

  implicit class GenomicChromRDD(rdd: RDD[((Long,String), Iterable[GRECORD])]) {
    def GMap(Ref_broadcast:Map[(Long, String), Iterable[(GRecordKey, Array[GValue])]] ,
             offset_l: Int, offset_r: Int,outOp: List[RegionAggregate.RegionsToRegion],groups:groupType,bin:Int,sc:SparkContext): RDD[GRECORD] = {
      //logger.info("ScanMap is called :) \n\n")

      val accurateMap = rdd.flatMap { exp_chrom =>
        val e_chrom = if (exp_chrom._1._2.indexOf("_") > 0) exp_chrom._1._2.substring(0, exp_chrom._1._2.indexOf("_")) else exp_chrom._1._2

        groups.map(x=>(x._1._1,x._2)).distinct.flatMap { x =>
          if (x._2.contains(exp_chrom._1._1)) {
            val ref_Chrom = Ref_broadcast.get((x._1, exp_chrom._1._2)).getOrElse(Iterable.empty);
            if(!ref_Chrom.isEmpty) {

//              val newID = Hashing.md5().newHasher().putLong(x._1).putLong(exp_chrom._1._1).hash().asLong()
              val chrom_out = GMAP.scanMapChroms(ref_Chrom.iterator, exp_chrom._2.iterator, outOp, offset_l, offset_r,bin)
              Some(((x._1,exp_chrom._1._1,exp_chrom._1._2),chrom_out.toIterable))
            }else
              None
          }else
          {
            None
          }
        }
      }


      //TODO  clear the bellow Code (Generate emtpy ref easier)
      val s = groups.flatMap{x=>x._2.map{r=>val newID = (x._1._1,r,x._1._2);(newID,())}}
      val ss = sc.parallelize(s)

      val expSig = accurateMap.keys.distinct.map(x=>((x._1,x._2,x._3),1))
      val d = ss.leftOuterJoin(expSig).filter{x=> //val newID = Hashing.md5().newHasher().putLong(x._1._1).putLong(x._1._2).hash().asLong();println("GF",newID,x,x._2._2.getOrElse(0)==0) ;
        x._2._2.getOrElse(0)==0}.keys
      val outextra= d.flatMap{x=>
        val newID = Hashing.md5().newHasher().putLong(x._1).putLong(x._2).hash().asLong();
        val newValues = outOp.map{x=>x.fun(List())}.toArray
        Ref_broadcast.get((x._1, x._3)).getOrElse(Iterable.empty).map(s=>(new GRecordKey(newID,s._1._2,s._1._3,s._1._4,s._1._5),s._2++newValues));
      }

      val out = accurateMap.values.flatMap(x=>x) union(outextra)
      out
    }.setName("Genomic Map on Chromosomes")

    def ids(): Array[Long]= rdd.values.map(x=>x.iterator.next()._1._1).distinct().collect();

  }
}