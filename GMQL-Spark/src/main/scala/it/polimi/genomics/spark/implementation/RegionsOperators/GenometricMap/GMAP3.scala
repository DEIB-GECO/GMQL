package it.polimi.genomics.spark.implementation.RegionsOperators.GenometricMap

import com.google.common.base.Charsets
import com.google.common.hash.Hashing
import it.polimi.genomics.core.{GValue, GString, GDouble}
import it.polimi.genomics.core.DataStructures.RegionAggregate
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.GRecordKey
import it.polimi.genomics.core.exception.SelectFormatException
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory


/**
 * Created by abdulrahman kaitoua on 08/08/15.
 */
object GMAP3 {
  private final val logger = LoggerFactory.getLogger(this.getClass);

  @throws[SelectFormatException]
  def apply(aggregator : List[RegionAggregate.RegionsToRegion], flat : Boolean, ref : RDD[(Long, String, Long, Long, Char, Array[GValue])], exp : RDD[(Long, String, Long, Long, Char, Long, Array[GValue])], BINNING_PARAMETER : Long) : RDD[GRECORD] = {
    logger.info("----------------GMAP2 executing..")
    val refBinnedRep: RDD[((Long, String, Int), (Long, Long, Char, Array[GValue], Long))] =
      binDS(ref,BINNING_PARAMETER)

    val expBinned: RDD[((Long, String, Int), (Long, Long, Char, Array[GValue]))] =
      binDS(exp,aggregator,BINNING_PARAMETER)

    val extraData: List[Array[List[GValue]]] =
      List(aggregator.map(_ => List[GValue]()).toArray)

    val RefExpJoined = refBinnedRep.cogroup(expBinned)
      .flatMap { grouped =>
      val key = grouped._1;
      val references = grouped._2._1;
      val experiments = grouped._2._2
        val refCollected: List[PartialResult] = references.map(r => (new PartialResult(r, 0, /* List[Array[List[GValue]]]() */ extraData, -1L, -1L, -1L, -1L))).toList
        for (e <- experiments) {
          for (r <- refCollected) {
            if ( /* cross */
            /* space overlapping */
              (r.binnedRegion._1 < e._2 && e._1 < r.binnedRegion._2)
                && /* same strand */
                (r.binnedRegion._3.equals('*') || e._3.equals('*') || r.binnedRegion._3.equals(e._3))
                && /* first comparison */
                ((r.binnedRegion._1 / BINNING_PARAMETER).toInt.equals(key._3) || (e._1 / BINNING_PARAMETER).toInt.equals(key._3))
            ) {
              r.count += 1
              r.extra = r.extra :+ e._4.foldLeft(Array[List[GValue]]())((z: Array[List[GValue]], v: GValue) => z :+ List(v))

              val startMin =
                if(r.startUnion.equals(-1L)){
                  e._1
                } else if(e._1.equals(-1L)){
                  r.startUnion
                } else {
                  Math.min(r.startUnion, e._1)
                }

              val endMax =
                Math.max(r.endUnion, e._2)

              val startMax =
                Math.max(r.startIntersection, e._1)

              val endMin =
                if(r.endIntersection.equals(-1L)){
                  e._2
                } else if(e._2.equals(-1L)){
                  r.endIntersection
                } else {
                  Math.min(r.endIntersection, e._2)
                }

              r.startUnion = startMin
              r.endUnion = endMax
              r.startIntersection = startMax
              r.endIntersection = endMin

            }
          }
        }
        refCollected.map{pr =>
          (pr.binnedRegion._5, ((key._1, key._2, pr.binnedRegion._1, pr.binnedRegion._2, pr.binnedRegion._3), pr.binnedRegion._4, pr.extra.reduce((a, b) => a.zip(b).map((p) => p._1 ++ p._2)), pr.count, pr.startUnion, pr.endUnion, pr.startIntersection, pr.endIntersection))
        }
    }

    val reduced  = RefExpJoined
    .reduceByKey{(r1,r2) =>
      val startMin =
        if(r1._5.equals(-1L)){
          r2._5
        } else if(r2._5.equals(-1L)){
          r1._5
        } else {
          Math.min(r1._5, r2._5)
        }

      val endMax =
        Math.max(r1._6, r2._6)

      val startMax =
        Math.max(r1._7, r2._7)

      val endMin =
        if(r1._8.equals(-1L)){
          r2._8
        } else if(r2._8.equals(-1L)){
          r1._8
        } else {
          Math.min(r1._8, r2._8)
        }

      /*
      if(r1._2 == "chr3" && r1._3.equals(46214L)){
        println("------------------ERROR " + (startMin, endMax, if(startMax < endMin) startMax else 0L, if(endMin > startMax) endMin else 0L))
      }
      */

      val extra: Array[List[GValue]] =
        if(r1._3.isEmpty){
          r2._3
        } else if(r2._3.isEmpty){
          r1._3
        } else {
          r1._3
            .zip(r2._3)
            .map((a) => a._1 ++ a._2)
        }

      (r1._1, r1._2, extra,
        r1._4 + r2._4, startMin, endMax, if(startMax < endMin) startMax else 0L, if(endMin > startMax) endMin else 0L)
    }


//      .reduceByKey{(l,r)=>
//      val values: Array[List[GValue]] =
//        if(l._3.size > 0) {
//          (for(i <- (0 to (l._3.size-1)))
//            yield
//            (l._3(i) :::
//              r._3(i))).toArray
//        }
//          // l._3.zip(r._3).map((a) => a._1 ++ a._2)
//        else if(r._3.size >0)
//          r._3
//        else l._3
//      val startMin =Math.min(l._4, r._4)
//      val endMax = Math.max(l._5, r._5)
//      val startMax = Math.max(l._6, r._6)
//      val endMin = Math.min(l._7, r._7)
////      println("min/max ",startMax,endMax)
//      (l._1,l._2,values, startMin, endMax, if(startMax < endMin) startMax else 0L, if(endMin > startMax) endMin else 0L)
//    }

    //Aggregate Exp Values (reduced)

    val output = reduced.map{res =>
      var i =0;
      val start : Double = if(flat) res._2._5 else res._2._1._3
      val end : Double = if (flat) res._2._6 else res._2._1._4

//      println(res._2._8,res._2._7, res._2._8>res._2._7)
      val newVal:Array[GValue] = aggregator.map{f=> val out = f.fun(res._2._3(i));i = i+1; out}.toArray
      (new GRecordKey(res._2._1._1,res._2._1._2,start.toLong,end.toLong,res._2._1._5),
        res._2._2  ++ newVal
          // Jaccard 1
          :+ { if(res._2._6-res._2._5 != 0){ GDouble(Math.abs((end.toDouble-start)/(res._2._6-res._2._5))) } else { GDouble(0) } }
          // Jaccard 2
          :+ { if(res._2._6-res._2._5 != 0){ GDouble(Math.abs((res._2._8.toDouble-res._2._7)/(res._2._6-res._2._5))) } else { GDouble(0) } }
        )
    }

    output

    //    }else None
  }
  def binDS(rdd : RDD[(Long, String, Long, Long, Char, Long, Array[GValue])],aggregator: List[RegionAggregate.RegionsToRegion],bin: Long )
  : RDD[((Long, String, Int), (Long, Long, Char, Array[GValue]))] =
    rdd.flatMap { x =>

      val startbin = (x._3 / bin).toInt
      val stopbin = (x._4 / bin).toInt
      val newVal: Array[GValue] = aggregator
        .flatMap((f : RegionAggregate.RegionsToRegion) => {
        List(x._7(f.index))
      }).toArray
      ( startbin to stopbin).map( i=>
        ((x._1, x._2, i), (x._3, x._4, x._5, newVal))
      )
    }
  def binDS(rdd:RDD[(Long, String, Long, Long, Char, Array[GValue])],bin: Long )
  : RDD[((Long, String, Int), ( Long, Long, Char, Array[GValue],Long))] =
    rdd.flatMap { x =>

      val startbin = (x._3 / bin).toInt
      val stopbin = (x._4 / bin).toInt

      (startbin to stopbin).map { i =>
        val aggID = Hashing.md5().newHasher().putString("" + x._1 + x._2 + x._3 + x._4 + x._5 + x._6.mkString("/"), Charsets.UTF_8).hash().asLong
      ((x._1, x._2, i), (x._3, x._4, x._5, x._6,aggID))
    }
    }
  class PartialResult(val binnedRegion: (Long, Long, Char, Array[GValue], Long), var count: Int, var extra: List[Array[List[GValue]]],var startUnion : Long, var endUnion : Long, var startIntersection : Long, var endIntersection : Long) {
    override def toString(): String = {
      "PR" + binnedRegion.toString() + " --" + count + "-- " + extra.mkString("-")
    }
  }

}