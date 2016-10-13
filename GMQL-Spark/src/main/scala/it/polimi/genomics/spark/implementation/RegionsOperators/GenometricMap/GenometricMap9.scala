package it.polimi.genomics.spark.implementation.RegionsOperators.GenometricMap

import com.google.common.hash.Hashing
import it.polimi.genomics.core.DataStructures._
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.core.{GDouble, GRecordKey, GValue}
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.{HashPartitioner, Partitioner, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.collection.Map
import scala.collection.immutable.Iterable


/**
 * Created by abdulrahman kaitoua on 09/30/16.
  * more memory consumption, we run on a group ID not a couple of samples.
  * same partitioner for both refernece and experiment
 */
object GenometricMap9 {
  private final val logger = LoggerFactory.getLogger(this.getClass);
  private final type groupType = Array[((Long, String), Array[Long])]

  @throws[SelectFormatException]
  def apply(executor : GMQLSparkExecutor, grouping : OptionalMetaJoinOperator, aggregator : List[RegionAggregate.RegionsToRegion], reference : RegionOperator, experiments : RegionOperator, BINNING_PARAMETER:Long,REF_PARALLILISM:Int,sc : SparkContext) : RDD[GRECORD] = {
    logger.info("----------------MAP executing -------------")
    //creating the datasets
    val ref: RDD[(GRecordKey, Array[GValue])] =
      executor.implement_rd(reference, sc)
    val exp: RDD[(GRecordKey, Array[GValue])] =
      executor.implement_rd(experiments, sc)

    execute(executor, grouping, aggregator, ref, exp, BINNING_PARAMETER, REF_PARALLILISM, sc)
  }

  @throws[SelectFormatException]
  def execute(executor : GMQLSparkExecutor, grouping : OptionalMetaJoinOperator, aggregator : List[RegionAggregate.RegionsToRegion], ref: RDD[GRECORD], exp: RDD[GRECORD], BINNING_PARAMETER:Long, REF_PARALLILISM:Int,sc : SparkContext) : RDD[GRECORD] = {
    val groups = executor.implement_mjd(grouping, sc)
      .flatMap{x=>
        val ha = Hashing.md5.newHasher.putLong(x._1); x._2.foreach{SID => ha.putLong(SID)}; val group = ha.hash().asLong(); (x._1,group) :: x._2.map(s=>(s,group)).toList}

//    groups.foreach(println(_))

    val expDataPartitioner: Partitioner = exp.partitioner match {
      case (Some(p)) => p
      case (None) => new HashPartitioner(exp.partitions.length)
    }

    val expBinned = exp.binDS(BINNING_PARAMETER,aggregator,groups,expDataPartitioner)
    val refBinnedRep = ref.binDS(BINNING_PARAMETER,groups,expDataPartitioner).cache()
//    println(expBinned.first())
//    println(refBinnedRep.first())

    val RefExpJoined: RDD[(Long, (GRecordKey, Array[GValue], Array[GValue], Int))] = refBinnedRep.cogroup(expBinned)
      .flatMap { grouped => val key: (Long, String, Int) = grouped._1;
        val refList = grouped._2._1.toList.groupBy(ex => ex._1).map(ex=>(ex._1,ex._2.sortBy(x=>(x._1,x._2,x._3))))
        val expList = grouped._2._2.toList.groupBy(ex => ex._1).map(ex=>(ex._1,ex._2.sortBy(x=>(x._1,x._2,x._3))))
        expList.flatMap { exp => /*println(exp._1,key._2,key._3);*/
          refList.flatMap { ref =>
//            println("-----0-------");
            sweep((exp._1, key._2, key._3), ref._2.iterator, exp._2.iterator, BINNING_PARAMETER,aggregator).toList;
//            println("done");
//            println(s.size);
          }
//          println("Total", dddd.size)
        }
      }.cache()

//    RefExpJoined.foreach(println _)
//    RefExpJoined.map(x=>(x._2._1,x._2._3 :+ GDouble(x._2._4)))
    val reduced  = RefExpJoined.reduceByKey{(l,r)=>
      val values: Array[GValue] =
        if(l._3.size > 0 && r._3.size >0) {
          var i = -1;
          val dd = aggregator.map{a=> i+=1
            a.fun(List(l._3(i),r._3(i)))
           }.toArray
          dd
        }else if(r._3.size >0)
          r._3
        else l._3
      (l._1,l._2,values,l._4+r._4)
    }.cache()

//    RefExpJoined.unpersist(true)
    //Aggregate Exp Values (reduced)

    val output = reduced.map{res =>
      var i = -1;
      val newVal:Array[GValue] = aggregator.map{f=>i = i+1;val valList = if(res._2._3.size >0)res._2._3(i) else {GDouble(0.0000000000000001)}; f.funOut(valList,res._2._4)}.toArray
      (res._2._1,(res._2._2 :+ GDouble(res._2._4)) ++ newVal )
    }

//    reduced.unpersist()

    output
//    RefExpJoined.map(x=>(x._2._1,x._2._3 :+ GDouble(x._2._4)))
  }
  def sweep(key:(Long, String, Int),ref_regions:Iterator[(Long, Long, Long, Char, Array[GValue])],iExp:Iterator[(Long, Long, Long, Char, Array[GValue])]
            ,bin:Long , aggregator : List[RegionAggregate.RegionsToRegion]): Iterator[(Long, (GRecordKey, Array[GValue], Array[GValue], Int))] = {

    //init empty list for caching regions
    var RegionCache= List[(Long, Long, Long, Char, Array[GValue])]();
    var temp = List[(Long, Long, Long, Char, Array[GValue])]() ;
    var intersectings = List[(Long, Long, Long, Char, Array[GValue])]();

    var exp_region:(Long, Long, Long, Char, Array[GValue]) = (0l,0l,0l,'*',Array[GValue]())
    if(iExp.hasNext)
      exp_region = iExp.next;
    else
      logger.debug(s"Experiment got empty while it was not !!!")

    //println(ref_regions.size)
    ref_regions.map{ref_region =>
      //clear the intersection list
      intersectings = List.empty;
      temp = List.empty;

      //check the cache
      RegionCache.map{cRegion=>
        if (/* space overlapping */ref_region._2 < cRegion._3 && cRegion._2 < ref_region._3) {
          if (/* space overlapping */
            (ref_region._2 < cRegion._3 && cRegion._2 < ref_region._3)
              && /* same strand */
              (ref_region._4.equals('*') || cRegion._4.equals('*') || ref_region._4.equals(cRegion._4))
              && /* first comparison */
              checkBINCompatible(ref_region._2, cRegion._2 , bin,key._3))
          {
            intersectings ::=cRegion;
          }
          temp ::= cRegion;
        } else if (!(cRegion._3 < ref_region._2)) {
          temp ::= cRegion;
        }
      }

      RegionCache = temp;

      //iterate on exp regions. Break when no intersection
      //is found or when we overcome the current reference
      while (exp_region != null && ref_region._3  > exp_region._2) {
        if (/* space overlapping */ref_region._2 < exp_region._3 && exp_region._2 < ref_region._3) {
          //the region is inside, we process it
          if (/* space overlapping */
            (ref_region._2 < exp_region._3 && exp_region._2 < ref_region._3)
              && /* same strand */
              (ref_region._4.equals('*') || exp_region._4.equals('*') || ref_region._4.equals(exp_region._4))
              && /* first comparison */
              checkBINCompatible(ref_region._2,exp_region._2, bin,key._3)
          )
          {
            intersectings ::=exp_region;
          }
          RegionCache ::= exp_region;
        }

        if (iExp.hasNext) {
          // add to cache
          exp_region = iExp.next();
        } else {
          exp_region = null;
        }
      }// end while on exp regions
    val newID = Hashing.md5().newHasher().putLong(ref_region._1).putLong(key._1).hash().asLong()
      val aggregation = Hashing.md5().newHasher().putString(newID + key._2 + ref_region._2 + ref_region._3 + ref_region._4 + ref_region._5.mkString("/"), java.nio.charset.Charset.defaultCharset()).hash().asLong()

//      println(intersectings.size,ref_region,newID ,key._2 , ref_region._2 , ref_region._3 , ref_region._4 , ref_region._5.mkString("/"),"aggregation : "+aggregation)


      if(intersectings.size >0 ) {
        val intersect = intersectings.reduce{(l,r)=>
          val values: Array[GValue] =
            if(l._5.size > 0 && r._5.size >0) {
              var i = -1;
              val dd = aggregator.map{a=> i+=1
                a.fun(List(l._5(i),r._5(i)))
              }.toArray
              dd
            }else if(r._5.size >0)
              r._5
            else l._5
          (l._1,l._2,l._3,l._4,values)
        }

        (aggregation, (new GRecordKey(newID, key._2, ref_region._2, ref_region._3, ref_region._4), ref_region._5, intersect._5, intersectings.size))
      }else
        (aggregation, (new GRecordKey(newID, key._2, ref_region._2, ref_region._3, ref_region._4), ref_region._5, Array[GValue](), 0))

    }
  }

  def checkBINCompatible(rStart:Long,eStart:Long,binSize:Long,bin:Int): Boolean ={
    if(binSize >0)
      (rStart / binSize).toInt.equals(bin) || (eStart / binSize).toInt.equals(bin)
    else true
  }

  implicit class Binning(rdd: RDD[GRECORD]) {
    def binDS(bin: Long,aggregator: List[RegionAggregate.RegionsToRegion], Bgroups: RDD[(Long, Long)], partitioner: Partitioner): RDD[((Long, String, Int), (Long, Long, Long, Char, Array[GValue]))] =
      rdd.keyBy(x=>x._1._1).join(Bgroups/*,new HashPartitioner(Bgroups.count.toInt)*/).flatMap { x =>
        if (bin > 0) {
          val startbin = (x._2._1._1._3 / bin).toInt
          val stopbin = (x._2._1._1._4 / bin).toInt
          val newVal: Array[GValue] = aggregator
            .map((f: RegionAggregate.RegionsToRegion) => {
              x._2._1._2(f.index)
            }).toArray
          //          println (newVal.mkString("/"))
            for (i <- startbin to stopbin)
              yield ((x._2._2, x._2._1._1._2, i), (x._2._1._1._1, x._2._1._1._3, x._2._1._1._4, x._2._1._1._5, newVal))

        } else {
          val newVal: Array[GValue] = aggregator
            .map((f: RegionAggregate.RegionsToRegion) => {
              x._2._1._2(f.index)
            }).toArray
          //          println (newVal.mkString("/"))

          Some((x._2._2, x._2._1._1._2, 0), (x._2._1._1._1, x._2._1._1._3, x._2._1._1._4, x._2._1._1._5, newVal))

        }
      }//.partitionBy(partitioner)

    def binDS(bin: Long,Bgroups: RDD[(Long, Long)],partitioner: Partitioner )
    : RDD[((Long, String, Int), (Long, Long, Long, Char, Array[GValue]))] =
      rdd.keyBy(x=>x._1._1).join(Bgroups/*,new HashPartitioner(Bgroups.count.toInt)*/).flatMap { x =>
        if (bin > 0) {
          val startbin = (x._2._1._1._3 / bin).toInt
          val stopbin = (x._2._1._1._4 / bin).toInt
               (startbin to stopbin).map(i =>
                 ((x._2._2, x._2._1._1._2, i), (x._2._1._1._1, x._2._1._1._3, x._2._1._1._4, x._2._1._1._5, x._2._1._2))
              )
        }else
        {
              Some(((x._2._2, x._2._1._1._2, 0), (x._2._1._1._1, x._2._1._1._3, x._2._1._1._4, x._2._1._1._5, x._2._1._2)))
        }
      }//.partitionBy(partitioner)
  }
}