package it.polimi.genomics.spark.implementation.RegionsOperators.GenometricMap

import com.google.common.hash.Hashing
import it.polimi.genomics.core.DataStructures._
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.core.{GDouble, GRecordKey, GValue}
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.collection.Map


/**
 * Created by abdulrahman kaitoua on 08/08/15.
  * Map with reference broadcasted.
 */
object GenometricMap6 {
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

    execute(executor, grouping, aggregator, ref, exp, BINNING_PARAMETER,REF_PARALLILISM, sc)
  }

  @throws[SelectFormatException]
  def execute(executor : GMQLSparkExecutor, grouping : OptionalMetaJoinOperator, aggregator : List[RegionAggregate.RegionsToRegion], ref: RDD[GRECORD], exp: RDD[GRECORD], BINNING_PARAMETER:Long, REF_PARALLILISM:Int,sc : SparkContext) : RDD[GRECORD] = {
    val groups: Map[Long, Array[Long]] = executor.implement_mjd(grouping, sc).collectAsMap()

    val refGroups: Option[Broadcast[Map[Long, Array[Long]]]] = if(groups.isEmpty)None;
      else Some(sc.broadcast(groups))
    val numPartitions = groups.values.foldLeft(Array[Long]())((z,x)=>x++z).distinct.size


    class expPartitioner(partitions: Int) extends HashPartitioner(partitions) {

      override def getPartition(key: Any): Int = key match {
        case k: Tuple3[Long, String,Int] => super.getPartition(k._1)
        case _ => super.getPartition(key)
      }
    }

    val expBinned = exp.binDS(BINNING_PARAMETER,aggregator)//.partitionBy(new expPartitioner(numPartitions))
    val refBinnedRep = sc.broadcast(ref.binDS(BINNING_PARAMETER).groupBy(x=>(x._1._2,x._1._3)).collectAsMap)



    val RefExpJoined: RDD[(Long, (GRecordKey, Array[GValue], Array[GValue], Int))] = expBinned.mapPartitions({ expPartition =>
      val refBin = refBinnedRep.value

      def intersection(refRegion: ((Long, String, Int), (Long, Long, Long, Char, Array[GValue])),expRegion: ((Long, String, Int), (Long, Long, Char, Array[GValue]))  )={
        val newID = Hashing.md5().newHasher().putLong(refRegion._1._1).putLong(expRegion._1._1).hash().asLong()
        val aggregation = Hashing.md5().newHasher().putString(newID + refRegion._1._2 + refRegion._2._2 + refRegion._2._3 + refRegion._2._4 + refRegion._2._5.mkString("/"), java.nio.charset.Charset.defaultCharset()).hash().asLong()
        if( /* space overlapping */
          (refRegion._2._2 < expRegion._2._2 && expRegion._2._1 < refRegion._2._3)
            && /* same strand */
            (refRegion._2._4.equals('*') || expRegion._2._3.equals('*') || refRegion._2._4.equals(expRegion._2._3))
            && /* first comparison */
            ((refRegion._2._2/BINNING_PARAMETER).toInt.equals(refRegion._1._3) ||  (expRegion._2._1/BINNING_PARAMETER).toInt.equals(expRegion._1._3))
        ){

          (aggregation,(new GRecordKey(newID,refRegion._1._2,refRegion._2._2,refRegion._2._3,refRegion._2._4),refRegion._2._5,expRegion._2._4,1))
        }else
          (aggregation,(new GRecordKey(newID, refRegion._1._2,refRegion._2._2,refRegion._2._3,refRegion._2._4),refRegion._2._5,Array[GValue](),0))

      }

      for {
        expRegion <- expPartition
        refRegion<- refBin.get((expRegion._1._2,expRegion._1._3)).getOrElse(Iterator.empty)
      } yield intersection(refRegion,expRegion)

//      val key = grouped._1;
//      val ref = grouped._2._1;
//      val exp = grouped._2._2
//      val newID = Hashing.md5().newHasher().putLong(ref._1).putLong(key._1).hash().asLong()
//      val aggregation = Hashing.md5().newHasher().putString(newID + key._2 + ref._2 + ref._3 + ref._4 + ref._5.mkString("/"), java.nio.charset.Charset.defaultCharset()).hash().asLong()
      //        if (!exp.isDefined) {
      //          (aggregation,(new GRecordKey(newID, key._2,ref._2,ref._3,ref._4),ref._5,Array[GValue](),0))
      //      } else {
      //        val e = exp.get
      //        if( /* space overlapping */
      //          (ref._2 < e._2 && e._1 < ref._3)
      //            && /* same strand */
      //            (ref._4.equals('*') || e._3.equals('*') || ref._4.equals(e._3))
      //            && /* first comparison */
      //            ((ref._2/BINNING_PARAMETER).toInt.equals(key._3) ||  (e._1/BINNING_PARAMETER).toInt.equals(key._3))
      //        )
      //          (aggregation,(new GRecordKey(newID,key._2,ref._2,ref._3,ref._4),ref._5,exp.get._4,1))
      //        else
      //          (aggregation,(new GRecordKey(newID, key._2,ref._2,ref._3,ref._4),ref._5,Array[GValue](),0))
      //      }
    }
    ,preservesPartitioning = true)//.cache()

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
    }//cache()

    RefExpJoined.unpersist(true)
    //Aggregate Exp Values (reduced)

    val output = reduced.map{res =>
      var i = -1;
      val newVal:Array[GValue] = aggregator.map{f=>i = i+1;val valList = if(res._2._3.size >0)res._2._3(i) else {GDouble(0.0000000000000001)}; f.funOut(valList,res._2._4)}.toArray
      (res._2._1,(res._2._2 :+ GDouble(res._2._4)) ++ newVal )
    }

    reduced.unpersist()

    output
  }


  implicit class Binning(rdd: RDD[GRECORD]) {
    def binDS(bin: Long,aggregator: List[RegionAggregate.RegionsToRegion]): RDD[((Long, String, Int), (Long, Long, Char, Array[GValue]))] =
        rdd.flatMap { x =>
          val startbin =(x._1._3 / bin).toInt
          val stopbin = (x._1._4  / bin).toInt
          val newVal: Array[GValue] = aggregator
            .map((f : RegionAggregate.RegionsToRegion) => {
            x._2(f.index)
          }).toArray
//          println (newVal.mkString("/"))
          for (i <- startbin to stopbin)
            yield ((x._1._1, x._1._2, i),(x._1._3, x._1._4, x._1._5, newVal))
        }

    def binDS(bin: Long ): RDD[((Long, String, Int), (Long, Long, Long, Char, Array[GValue]))] =
      rdd.flatMap { x =>
        val startbin = (x._1._3 / bin).toInt
        val stopbin = (x._1._4 / bin).toInt
        (startbin to stopbin).map(i =>
          (((x._1._1, x._1._2, i), (x._1._1, x._1._3, x._1._4, x._1._5, x._2)))
        )
      }
  }

}