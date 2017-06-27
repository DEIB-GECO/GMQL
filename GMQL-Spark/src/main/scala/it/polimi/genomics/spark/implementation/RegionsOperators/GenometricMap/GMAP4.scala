package it.polimi.genomics.spark.implementation.RegionsOperators.GenometricMap

import com.google.common.hash.Hashing
import it.polimi.genomics.core.DataStructures.RegionAggregate
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.{GDouble, GNull, GRecordKey, GValue}
import it.polimi.genomics.core.exception.SelectFormatException
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory


/**
 * Created by abdulrahman kaitoua on 08/08/15.
  * aggregation on two steps
 */
object GMAP4 {
  private final val logger = LoggerFactory.getLogger(this.getClass);

  @throws[SelectFormatException]
  def apply(aggregator : List[RegionAggregate.RegionsToRegion], flat : Boolean, summit:Boolean, ref : RDD[FlinkRegionType], exp : RDD[(Long, String, Long, Long, Char, Long, Array[GValue])], BINNING_PARAMETER : Long) : RDD[GRECORD] = {
    logger.info("----------------GMAP4 executing..")
    val expBinned = binDS(exp,aggregator,BINNING_PARAMETER)
    val refBinnedRep = binDS(ref,BINNING_PARAMETER)

//    refBinnedRep.collect().foreach(println _)
//    expBinned.collect().foreach(println _)
    val RefExpJoined = refBinnedRep.leftOuterJoin(expBinned)
      .flatMap { grouped =>
      val key = grouped._1;
      val ref = grouped._2._1;
      val exp = grouped._2._2
      val aggregation = Hashing.md5().newHasher().putString(key._1 + key._2 + ref._1 + ref._2+ref._3+ref._4.mkString("/"),java.nio.charset.Charset.defaultCharset()).hash().asLong()
      if (!exp.isDefined) {
        None//(aggregation,((key._1, key._2,ref._1,ref._2,ref._3),ref._4,Array[List[GValue]](),0l,0l,0l,0l))
      } else {
        val e = exp.get
//        println((ref._1 < e._2 && e._1 < ref._2),(ref._3.equals('*') || e._3.equals('*') || ref._3.equals(e._3)),((ref._1/BINNING_PARAMETER).toInt.equals(key._3) ||  (e._1/BINNING_PARAMETER).toInt.equals(key._3)))

        if(/* cross */
        /* space overlapping */
          (ref._1 < e._2 && e._1 < ref._2)
            && /* same strand */
            (ref._3.equals('*') || e._3.equals('*') || ref._3.equals(e._3))
            && /* first comparison */
            ((ref._1/BINNING_PARAMETER).toInt.equals(key._3) ||  (e._1/BINNING_PARAMETER).toInt.equals(key._3))
        )
         Some (aggregation,((key._1,key._2,ref._1,ref._2,ref._3),ref._4,exp.get._4,exp.get._1,exp.get._2,exp.get._1,exp.get._2,(1, exp.get._4.map(s=>if(s.isInstanceOf[GNull]) 0 else 1).iterator.toArray)))
        else None
//          (aggregation,((key._1, key._2,ref._1,ref._2,ref._3),ref._4,Array[List[GValue]](),0l,0l,0l,0l))
      }
    }
    // print out the resutl for debuging
    //    RefExpJoined.collect().map(x=>println ("after left join: "+x._1,x._2._1,x._2._2.mkString("/"),x._2._3.size,x._2._4))

//    RefExpJoined.collect.foreach(println(_))

    val reduced  = RefExpJoined.reduceByKey{(l,r)=>
      val values: Array[GValue] =
        if(l._3.size > 0 && r._3.size >0) {
          var i = -1;
          val dd = aggregator.map{a=> i+=1
            a.fun(List(l._3(i),r._3(i)))
          }.toArray
          dd
        } else if(r._3.size >0)
          r._3
        else l._3
      val startMin =Math.min(l._4, r._4)
      val endMax = Math.max(l._5, r._5)
      val startMax = Math.max(l._6, r._6)
      val endMin = Math.min(l._7, r._7)
//      println("min/max ",startMax,endMax)
//      if(!summit)
      (l._1,l._2,values,startMin, endMax, if(startMax < endMin) startMax else 0L, if(endMin > startMax) endMin else 0L, (l._8._1+r._8._1, l._8._2.zip(r._8._2).map(s=>s._1+s._2).iterator.toArray))
//      else
//        (l._1,l._2,values,startMin, endMax, if(startMax < endMin) startMax else 0L, if(endMin > startMax) endMin else 0L, l._8+r._8)
    }

    //Aggregate Exp Values (reduced)

    val output = reduced.map{res =>
      var i = -1;
      val start : Double = if(flat) res._2._4 else res._2._1._3
      val end : Double = if (flat) res._2._5 else res._2._1._4

//      val newVal:Array[GValue] = aggregator.map{f=> val out = f.fun(res._2._3(i));i = i+1; out}.toArray
      val newVal:Array[GValue] = aggregator.map{f=>i = i+1;val valList = if(res._2._3.size >0)res._2._3(i) else {GDouble(0.0000000000000001)}; f.funOut(valList,(res._2._8._1, if(res._2._3.size >0) res._2._8._2(i) else 0))}.toArray
      (new GRecordKey(res._2._1._1,res._2._1._2,start.toLong,end.toLong,res._2._1._5),
        (res._2._2
          // Jaccard 1
          :+ { if(res._2._5-res._2._4 != 0){ GDouble(Math.abs((end.toDouble-start)/(res._2._5-res._2._4))) } else { GDouble(0) } }
          // Jaccard 2
          :+ { if(res._2._5-res._2._4 != 0){ GDouble(Math.abs((res._2._7.toDouble-res._2._6)/(res._2._5-res._2._4))) } else { GDouble(0) } }) ++ newVal
        )
    }

    output

    //    }else None
  }

  def binDS(rdd : RDD[(Long, String, Long, Long, Char, Long, Array[GValue])],aggregator: List[RegionAggregate.RegionsToRegion], bin: Long)
  : RDD[((Long, String, Int), (Long, Long, Char, Array[GValue]))] =
    rdd.flatMap { x =>
      if (bin > 0) {
        val startbin = (x._3 / bin).toInt
        val stopbin = (x._4 / bin).toInt
        val newVal: Array[GValue] = aggregator
          .map((f: RegionAggregate.RegionsToRegion) => {
            x._7(f.index)
          }).toArray
        //          println (newVal.mkString("/"))
        for (i <- startbin to stopbin)
          yield ((x._1, x._2, i), (x._3, x._4, x._5, newVal))
      } else
      {
        val newVal: Array[GValue] = aggregator
          .map((f: RegionAggregate.RegionsToRegion) => {
            x._7(f.index)
          }).toArray
        //          println (newVal.mkString("/"))
        Some((x._1, x._2, 0), (x._3, x._4, x._5, newVal))
      }
    }

  def binDS(rdd:RDD[FlinkRegionType],bin: Long )
  : RDD[((Long, String, Int), ( Long, Long, Char, Array[GValue]))] =
    rdd.flatMap { x =>
      if (bin > 0) {
          val startbin = (x._3 / bin).toInt
          val stopbin = (x._4 / bin).toInt
            (startbin to stopbin).map(i =>
             ((x._1, x._2, i), ( x._3, x._4, x._5, x._6))
            )
      }else
      {
             List(((x._1, x._2, 0), ( x._3, x._4, x._5, x._6)))
      }
    }
}