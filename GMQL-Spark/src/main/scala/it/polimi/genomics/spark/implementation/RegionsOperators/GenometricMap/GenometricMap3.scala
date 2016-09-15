package it.polimi.genomics.spark.implementation.RegionsOperators.GenometricMap

import com.google.common.hash.Hashing
import it.polimi.genomics.core.DataStructures._
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.core.{DataTypes, GRecordKey}
import it.polimi.genomics.core.{GValue, GDouble}
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.Predef
import scala.collection.Map


/**
 * Created by abdulrahman kaitoua on 08/08/15.
 */
object GenometricMap3 {
  private final val logger = LoggerFactory.getLogger(this.getClass);
  private final type groupType = Array[((Long, String), Array[Long])]

  @throws[SelectFormatException]
  def apply(executor : GMQLSparkExecutor, grouping : OptionalMetaJoinOperator, aggregator : List[RegionAggregate.RegionsToRegion], reference : RegionOperator, experiments : RegionOperator, BINNING_PARAMETER:Long,REF_PARALLILISM:Int,sc : SparkContext) : RDD[GRECORD] = {
    logger.info("----------------MAP executing..")
    //creating the datasets
    val ref: RDD[(GRecordKey, Array[GValue])] =
      executor.implement_rd(reference, sc)
    val exp: RDD[(GRecordKey, Array[GValue])] =
      executor.implement_rd(experiments, sc)

    execute(executor, grouping, aggregator, ref, exp, BINNING_PARAMETER,REF_PARALLILISM, sc)
  }

  @throws[SelectFormatException]
  def execute(executor : GMQLSparkExecutor, grouping : OptionalMetaJoinOperator, aggregator : List[RegionAggregate.RegionsToRegion], ref: RDD[GRECORD], exp: RDD[GRECORD], BINNING_PARAMETER:Long, REF_PARALLILISM:Int,sc : SparkContext) : RDD[GRECORD] = {
    //creating the groups if they exist
    //otherwise create all possible (ref,Array[exp]) pairs
    val groups: Map[Long, Array[Long]] =
//      if (grouping.isInstanceOf[SomeMetaJoinOperator])
        executor.implement_mjd(grouping, sc).collectAsMap()
//      else ref.map(_._1._1).distinct.cartesian(exp.map(_._1._1).distinct()).groupByKey().mapValues(x=>x.toArray).collectAsMap()

//    groups.foreach(x=>x._2.foreach(s=>println("The ids froups ",x._1,s)))
    val refGroups: Option[Broadcast[Map[Long, Array[Long]]]] = if(groups.isEmpty)None; else Some(sc.broadcast(groups))
    val expBinned = exp.binDS(BINNING_PARAMETER,aggregator)
    val refBinnedRep = ref.binDS(BINNING_PARAMETER,refGroups)

    val RefExpJoined = refBinnedRep.leftOuterJoin(expBinned)
      .map { grouped => val key = grouped._1; val ref = grouped._2._1; val exp = grouped._2._2
      val newID = Hashing.md5().newHasher().putLong(ref._1).putLong(key._1).hash().asLong()
      val aggregation = Hashing.md5().newHasher().putString(newID + key._2 + ref._2 + ref._3+ref._4+ref._5.mkString("/"),java.nio.charset.Charset.defaultCharset()).hash().asLong()
      if (!exp.isDefined) {
        (aggregation,(new GRecordKey(newID, key._2,ref._2,ref._3,ref._4),ref._5,Array[List[GValue]](),0))
      } else {
        val e = exp.get
        if(/* cross */
        /* space overlapping */
          (ref._2 < e._2 && e._1 < ref._3)
            && /* same strand */
            (ref._4.equals('*') || e._3.equals('*') || ref._4.equals(e._3))
            && /* first comparison */
            ((ref._2/BINNING_PARAMETER).toInt.equals(key._3) ||  (e._1/BINNING_PARAMETER).toInt.equals(key._3))
        )
          (aggregation,(new GRecordKey(newID,key._2,ref._2,ref._3,ref._4),ref._5,exp.get._4,1))
        else
          (aggregation,(new GRecordKey(newID, key._2,ref._2,ref._3,ref._4),ref._5,Array[List[GValue]](),0))
      }
    }
// print out the resutl for debuging
//    RefExpJoined.collect().map(x=>println ("after left join: "+x._1,x._2._1,x._2._2.mkString("/"),x._2._3.size,x._2._4))

    val reduced  = RefExpJoined.reduceByKey{(l,r)=>
//      l._3.foreach(x=>println("l ",x.mkString("/") ))
//      r._3.foreach(x=>println("r ",x.mkString("/") ))
//        r._3)
      val values: Array[List[GValue]] =
        if(l._3.size > 0 && r._3.size >0)
          (for(i <- (0 to (l._3.size-1)))
          yield
            (l._3(i) :::
            r._3(i))).toArray
        else if(r._3.size >0)
          r._3
        else l._3
      (l._1,l._2,values,l._4+r._4)
    }

    //Aggregate Exp Values (reduced)

    val output = reduced.map{res =>
      var i =0;
//      println("Aggregator size : ",aggregator.size)
//      res._2._3.foreach(x=>println("res ",x.mkString("/") ))
      val newVal:Array[GValue] = aggregator.map{f=>val valList = if(res._2._3.size >0)res._2._3(i) else {/*println ("Ooops");*/List[GValue]()}; val out = f.fun(valList);i = i+1; out}.toArray
      (res._2._1,(res._2._2 :+ GDouble(res._2._4)) ++ newVal )
    }

    output

    //    }else None
  }


  implicit class Binning(rdd: RDD[GRECORD]) {
    def binDS(bin: Long,aggregator: List[RegionAggregate.RegionsToRegion]): RDD[((Long, String, Int), (Long, Long, Char, Array[List[GValue]]))] =
        rdd.flatMap { x =>
          val startbin =(x._1._3 / bin).toInt
          val stopbin = (x._1._4  / bin).toInt
          val newVal: Array[List[GValue]] = aggregator
            .map((f : RegionAggregate.RegionsToRegion) => {
            List(x._2(f.index))
          }).toArray
//          println (newVal.mkString("/"))
          for (i <- startbin to stopbin)
            yield ((x._1._1, x._1._2, i),(x._1._3, x._1._4, x._1._5, newVal))
        }

    def binDS(bin: Long,Bgroups: Option[Broadcast[Map[Long, Array[Long]]]] ): RDD[((Long, String, Int), (Long, Long, Long, Char, Array[GValue]))] =
      rdd.flatMap { x =>


        if (Bgroups.isDefined) {
        val startbin = (x._1._3 / bin).toInt
        val stopbin = (x._1._4 / bin).toInt
        val group = Bgroups.get.value.get(x._1._1)
          if(group.isDefined)
        (startbin to stopbin).flatMap(i =>
          group.get.map(id => (((id, x._1._2, i), (x._1._1, x._1._3, x._1._4, x._1._5, x._2))))
        ) else None
      }
        else None
      }
  }

}