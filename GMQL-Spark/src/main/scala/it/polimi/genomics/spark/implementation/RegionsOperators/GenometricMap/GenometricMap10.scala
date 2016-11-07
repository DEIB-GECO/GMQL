package it.polimi.genomics.spark.implementation.RegionsOperators.GenometricMap

import com.google.common.hash.Hashing
import it.polimi.genomics.core.DataStructures._
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.core.{GDouble, GRecordKey, GValue}
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkContext}
import org.slf4j.LoggerFactory

import scala.collection.Map


/**
 * Created by abdulrahman kaitoua on 09/30/16.
  * more memory consumption, we run on a group ID not a couple of samples.
  * same partitioner for both refernece and experiment
 */
object GenometricMap10 {
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

    execute(executor, grouping, aggregator, ref, exp, 100, REF_PARALLILISM, sc)
  }

  @throws[SelectFormatException]
  def execute(executor : GMQLSparkExecutor, grouping : OptionalMetaJoinOperator, aggregator : List[RegionAggregate.RegionsToRegion], ref: RDD[GRECORD], exp: RDD[GRECORD], BINNING_PARAMETER:Long, REF_PARALLILISM:Int,sc : SparkContext) : RDD[GRECORD] = {
    val g = executor.implement_mjd(grouping, sc)

    val expIDs: List[Long] = g.flatMap(_._2).distinct().collect().toList

    val groups: Broadcast[Map[Long, Iterable[Long]]] =
      sc.broadcast(
        g.flatMap { x =>
          val ha = Hashing.md5.newHasher.putLong(x._1);
          x._2.foreach { SID => ha.putLong(SID) };
          val group = ha.hash().asLong();
          (x._1, group) :: x._2.map(s => (s, group)).toList
        }.groupByKey.collectAsMap()
      )

    val expDataPartitioner: Partitioner = exp.partitioner match {
      case (Some(p)) => p
      case (None) => new HashPartitioner(exp.partitions.length)
    }

    logger.info("Reference: "+ref.count)
    logger.info("experiments: "+exp.count)
    groups.value.foreach(x=>logger.info(x._1+","+x._2.mkString("\t")))
    logger.info(groups.value.size.toString)

    val expBinned = exp.binDS(BINNING_PARAMETER,aggregator,groups,expDataPartitioner)
    val refBinnedRep = ref.binDS(BINNING_PARAMETER,groups,expDataPartitioner).cache()

    logger.info("reference Binned: "+expBinned.count())
      logger.info("experiments Binned: "+refBinnedRep.count)
//    println(expBinned.first())
//    println(refBinnedRep.first())

    val RefExpJoined: RDD[(Long, (GRecordKey, Array[GValue], Array[GValue], Int))] = refBinnedRep.cogroup(expBinned)
      .flatMap { grouped => val key: (Long, String, Int) = grouped._1;
        val ref: List[(Long, Long, Long, Char, Array[GValue])] = grouped._2._1.toList.sortBy(x=>(x._2,x._3))
        val expList = grouped._2._2.toList.groupBy(ex => ex._1).toList.map(ex=>(ex._1,ex._2.sortBy(x=>(x._1,x._2,x._3))))


        val sss=
          if(!expList.isEmpty)
            expList.flatMap { exp => /*println(exp._1,key._2,key._3);*/
            val ref_maped_exp = sweep((exp._1, key._2, key._3), ref, exp._2, BINNING_PARAMETER,aggregator);
              val outIDs = ref_maped_exp.map(_._2._1._1).distinct
              if(outIDs.size < expIDs.size) {
                 val extracted =  expIDs diff outIDs
                println("extracted: ",expIDs.mkString("\t"), outIDs.mkString("\t"),extracted.mkString("\t"))
                extracted.flatMap(id => sweep((id, key._2, key._3), ref, List(), BINNING_PARAMETER, aggregator))
              }
              else
//            logger.info("sweep: "+ref_maped_exp.size);
                ref_maped_exp
        }
        else {
            val rr = expIDs.flatMap(id => sweep((id, key._2, key._3), ref, List(), BINNING_PARAMETER, aggregator))
            rr
        }
//        logger.info("TT:"+ sss.size)
        if(expList.isEmpty)logger.info("ref size: "+ref.size+",exp list: "+expList.size+",exp size: "+0+" , TT:"+ sss.size)
        else logger.info("ref size: "+ref.size+",exp list: "+expList.size+",exp size: "+expList.head._2.size+" , TT:"+ sss.size)
        sss
      }//.cache()
    logger.info("TTT:"+RefExpJoined.count())

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
    }//cache()

//    RefExpJoined.unpersist(true)
    //Aggregate Exp Values (reduced)

    val output = reduced.map{res =>
      var i = -1;
      val newVal:Array[GValue] = aggregator.map{f=>i = i+1;val valList = if(res._2._3.size >0)res._2._3(i) else {GDouble(0.0000000000000001)}; f.funOut(valList,res._2._4)}.toArray
      (res._2._1,(res._2._2 :+ GDouble(res._2._4)) ++ newVal )
    }

//    reduced.unpersist()

    logger.info("output: "+ output.count)
    output
//    RefExpJoined.map(x=>(x._2._1,x._2._3 :+ GDouble(x._2._4)))
  }
  def sweep(key:(Long, String, Int),ref_regions:List[(Long, Long, Long, Char, Array[GValue])],expBin:List[(Long, Long, Long, Char, Array[GValue])]
            ,bin:Long , aggregator : List[RegionAggregate.RegionsToRegion]): List[(Long, (GRecordKey, Array[GValue], Array[GValue], Int))] = {

    //init empty list for caching regions
    var RegionCache= List[(Long, Long, Long, Char, Array[GValue])]();
    var temp = List[(Long, Long, Long, Char, Array[GValue])]() ;
    var intersectings = List[(Long, Long, Long, Char, Array[GValue])]();

    val iExp = if(expBin.isEmpty) Iterator.empty else expBin.iterator
    var exp_region:(Long, Long, Long, Char, Array[GValue]) = if(iExp.hasNext) {
      val dd = iExp.next;logger.info(s"full  !!!"+ dd);
      dd;
    } else{
      logger.info(s"Experiment got empty  !!!");
      null
    }


    logger.info("ref_Bin_size: "+ref_regions.size)
    logger.info("exp_Bin_size: "+expBin.size)

   val dd = ref_regions.map{ref_region =>
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

    logger.info("ref_Bin_size out: "+dd.size)
    dd
  }

  def checkBINCompatible(rStart:Long,eStart:Long,binSize:Long,bin:Int): Boolean ={
    if(binSize >0)
      (rStart / binSize).toInt.equals(bin) || (eStart / binSize).toInt.equals(bin)
    else true
  }

  implicit class Binning(rdd: RDD[GRECORD]) {
    def binDS(bin: Long,aggregator: List[RegionAggregate.RegionsToRegion], Bgroups: Broadcast[Map[Long, Iterable[Long]]], partitioner: Partitioner): RDD[((Long, String, Int), (Long, Long, Long, Char, Array[GValue]))] =
      rdd.flatMap { x =>
        val sampleKey = x._1
        if (bin > 0) {
          val startbin = (sampleKey._3 / bin).toInt
          val stopbin = (sampleKey._4 / bin).toInt
          val newVal: Array[GValue] = aggregator
            .map((f: RegionAggregate.RegionsToRegion) => {
              x._2(f.index)
            }).toArray
          //          println (newVal.mkString("/"))

          val group = Bgroups.value.get(x._1._1)
          if (group.isDefined)
            (startbin to stopbin).flatMap(i =>
              group.get.map(id => ((id, sampleKey._2, i), (sampleKey._1, sampleKey._3, sampleKey._4, sampleKey._5, newVal))
              )
            )
          else None
        } else {

          val newVal: Array[GValue] = aggregator
            .map((f: RegionAggregate.RegionsToRegion) => {
              x._2(f.index)
            }).toArray
          //          println (newVal.mkString("/"))

          val group = Bgroups.value.get(x._1._1)
          if (group.isDefined)
            group.get.map(id => ((id, sampleKey._2, 0), (sampleKey._1, sampleKey._3, sampleKey._4, sampleKey._5, newVal)))
          else None
        }
      }

    def binDS(bin: Long,Bgroups: Broadcast[Map[Long, Iterable[Long]]],partitioner: Partitioner )
    : RDD[((Long, String, Int), (Long, Long, Long, Char, Array[GValue]))] =
      rdd.flatMap { x =>
        if (bin > 0) {
          val startbin = (x._1._3 / bin).toInt
          val stopbin = (x._1._4 / bin).toInt
          val group = Bgroups.value.get(x._1._1)
          if (group.isDefined)
            (startbin to stopbin).flatMap(i =>
              group.get.map(id => ((id, x._1._2, i), (x._1._1, x._1._3, x._1._4, x._1._5, x._2))
              )
            )else None
        }else
        {
          val group = Bgroups.value.get(x._1._1)
          if (group.isDefined)
            group.get.map(id =>  (((id, x._1._2, 0), (x._1._1, x._1._3, x._1._4, x._1._5, x._2)))
            )
          else None
        }
      }//.partitionBy(partitioner)
  }
}