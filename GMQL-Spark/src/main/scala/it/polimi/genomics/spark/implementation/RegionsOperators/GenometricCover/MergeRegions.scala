package it.polimi.genomics.spark.implementation.RegionsOperators.GenometricCover

import it.polimi.genomics.core.{GDouble, GValue}
import it.polimi.genomics.core.GRecordKey
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.collection.Map

/**
 * Created by Abdulrahman Kaitoua on 30/04/15.
 */

object MergeRegions {

  final val logger = LoggerFactory.getLogger(this.getClass)
  final type GRECORD_COVER = ((Long, Int, String, Char), (Long, Long, Int, Array[GValue]))
  final type GRECORD_COVER_KEY = (Long, Int, String, Char) //the key: (groupID, Bin, Chrm, str)
  final type GRECORD_COVER_VALUE =  (Long, Long, Int, Array[GValue])  // value : (start,stop,startbin,values())

  implicit class MergeRegions(rdd: RDD[GRECORD_COVER]) {
    def sortWithPreservedPartitioning(ascending:Boolean = true): RDD[GRECORD_COVER] = {
      rdd.mapPartitions({
        partition =>
          val buf = partition.toArray
          if (ascending) {
            buf.sortBy(x => (x._1._1,x._1._2,x._1._3,x._1._4)).iterator
          } else {
            buf.sortBy(x => (x._1._1,x._1._2,x._1._3,x._1._4)).reverse.iterator
          }
      },preservesPartitioning = true)
    }

    def mergeRegions(min:Map[Long, Long],max:Map[Long, Long],withValues: Boolean = false): RDD[GRECORD_COVER] = {
      logger.info("----------------MergeRegions executing..")

      // mapping each partition (each chromosome,bin can be in only one partition)
      // Do Merge on every partition ( Hadoop Split ) separately
      rdd.mapPartitionsWithIndex ({
        // setup Code
        var Chrom: String = ""
        var regionCache = List[GRECORD_COVER]();

        // Partition is an iterator
        (i, partition) => {
          val s = partition.flatMap {
            record =>
              var temp = List[GRECORD_COVER]();
              if (!record._1._3.equals(Chrom)) {
                Chrom = record._1._3
                regionCache = List[GRECORD_COVER]()
                regionCache ::= (record._1,(record._2._1,record._2._2,record._2._3, if (withValues) record._2._4 :+ GDouble(1) else Array[GValue]() :+ GDouble(1)))
                None
              } else {
                if (regionCache.isEmpty)
                  temp ::=(record._1,(record._2._1,record._2._2,record._2._3, if (withValues) record._2._4 :+ GDouble(1) else Array[GValue]() :+ GDouble(1)))

                val cache = regionCache.iterator
                var res = cache.flatMap { cRegion =>
                  val out =
                    if (record._2._1 >= cRegion._2._2) {
                      val numIntersections = cRegion._2._4(cRegion._2._4.size-1).asInstanceOf[GDouble].v
                      if(numIntersections >= min.get(cRegion._1._1).get && numIntersections <= max.get(cRegion._1._1).get) Some(cRegion) else None
                    }
                    else if (cRegion._2._1 >= record._2._2) {
                      temp ::= cRegion;
                      None
                    }
                    else {
                      for (g <- splitRegions(cRegion, record, withValues)) temp ::= g;
                      None;
                    }
                  if (!cache.hasNext && record._2._2 > cRegion._2._2) {
                    val start = Math.max(record._2._1, cRegion._2._2);
                    temp ::= (record._1, (start, record._2._2,record._2._3, if (withValues) record._2._4 :+ GDouble(1) else Array[GValue]() :+ GDouble(1)))
                  }
                  out
                }.toArray

                regionCache = temp
                // cleanup Code
                // release The cached regions since they are unique and no more regions available
                if (!partition.hasNext) {
                  res = res ++ regionCache.flatMap{x=>
                    val numIntersections = x._2._4(x._2._4.size-1).asInstanceOf[GDouble].v
                    if(numIntersections >= min.get(x._1._1).get && numIntersections <= max.get(x._1._1).get) Some(x) else None
                  }
                }
                res
              }
          }
          s
        }
      }, preservesPartitioning = true)
    }.setName("Build Chromosomes Merging Index")


    def fuseIndexes(): RDD[GRECORD_COVER] = {
      rdd.mapPartitions ({
        var Chrom: String = ""
        var Start:Long = 0;
        var Laststop:Long = 0;
        var maxAcc = 0.0;

        partition =>
          val partitionResult = partition.flatMap {
            record =>
              val numIntersections = record._2._4(record._2._4.size-1).asInstanceOf[GDouble].v
              val fusedRegion =
                if (!record._1._2.equals(Chrom)) {
                  Chrom = record._1._3
                  maxAcc =0
                  val numIntersections = record._2._4(record._2._4.size-1).asInstanceOf[GDouble].v
                  //                  if(numIntersections >= lThreshold && numIntersections <= hThreshold) {
                  //println(record)
                  if(numIntersections>maxAcc) maxAcc=numIntersections
                  Start = record._2._1
                  Laststop = record._2._2
                  //                  }
                  None
                } else {

                  //                  if(numIntersections >= lThreshold && numIntersections <= hThreshold){
                  // println(record)
                  if(numIntersections>maxAcc){ maxAcc=numIntersections;}
                  if(record._2._2==Laststop){
                    Laststop = record._2._2
                    None
                  }
                  else
                  {
                    val out = if(Start !=0){
                      Some(((record._1._1,record._1._2,Chrom,record._1._4),(Start,Laststop,record._2._3,Array[GValue](GDouble(maxAcc)))))
                    }else None

                    maxAcc =0
                    Start = record._2._1
                    Laststop = record._2._2

                    out
                  }
                  //                  }else {None}
                }
              var output = List(fusedRegion)
              if (!partition.hasNext) {
                val out = ((record._1._1,record._1._2,Chrom,record._1._4),(Start,Laststop,record._2._3,Array[GValue](GDouble(maxAcc))))
                output = output :+ Some(out)
              }
              output
          }.flatMap(x=>x)
          partitionResult
      }, preservesPartitioning = true)
    }.setName("Fuse Region's Indexes with Threshold")

    def summit(): RDD[GRECORD_COVER] = {
      rdd.mapPartitions ({
        var Chrom: String = ""
        var Start:Long = 0;
        var Laststop:Long = 0;
        var  summitstart = 0l
        var summitstop = 0l
        var maxAcc = 0.0;

        partition =>
          val partitionResult = partition.flatMap {
            record =>
              val numIntersections = record._2._4(record._2._4.size-1).asInstanceOf[GDouble].v
              val fusedRegion =
                if (!record._1._2.equals(Chrom)) {
                  Chrom = record._1._3
                  maxAcc =0
                  val numIntersections = record._2._4(record._2._4.size-1).asInstanceOf[GDouble].v
                  //                  if(numIntersections >= lThreshold && numIntersections <= hThreshold) {
                  //println(record)
                  if(numIntersections>maxAcc) {summitstart = record._2._1;summitstop = record._2._2;maxAcc=numIntersections}
                  Start = record._2._1
                  Laststop = record._2._2
                  //                  }
                  None
                } else {

                  //                  if(numIntersections >= lThreshold && numIntersections <= hThreshold){
                  // println(record)
                  if(numIntersections>maxAcc){ summitstart = record._2._1;summitstop = record._2._2;maxAcc=numIntersections;}
                  if(record._2._2==Laststop){
                    Laststop = record._2._2
                    None
                  }
                  else
                  {
                    val out = if(Start !=0){
                      Some(((record._1._1,record._1._2,Chrom,record._1._4),(summitstart,summitstop,record._2._3,Array[GValue](GDouble(maxAcc)))))
                    }else None

                    maxAcc =0
                    Start = record._2._1
                    Laststop = record._2._2

                    out
                  }
                  //                  }else {None}
                }
              var output = List(fusedRegion)
              if (!partition.hasNext) {
                val out = ((record._1._1,record._1._2,Chrom,record._1._4),(summitstart,summitstop,record._2._3,Array[GValue](GDouble(maxAcc))))
                output = output :+ Some(out)
              }
              output
          }.flatMap(x=>x)
          partitionResult
      }, preservesPartitioning = true)
    }.setName("Summit Region's with Threshold")

    def splitRegions(r1: GRECORD_COVER,r2: GRECORD_COVER, withValues:Boolean):List[GRECORD_COVER]={
      var toRet =List[GRECORD_COVER]();
      val r1CountInd = r1._2._4.size-1
      val values = if(withValues)  r1._2._4 else Array[GValue]() :+ r1._2._4(r1CountInd)
      var values_plus= Array[GValue]();
      if(withValues)for (i<- 0 to r1._2._4.size-2)values_plus= values_plus :+ r1._2._4(i);
      values_plus =values_plus:+ GDouble(r1._2._4(r1CountInd).asInstanceOf[GDouble].v+1)

      if (r1._2._2 > r2._2._1 && r2._2._2 >= r1._2._2 && r1._2._1 < r2._2._1) {
        toRet::= (r1._1,(r1._2._1, r2._2._1,r2._2._3,values))
        toRet::= (r1._1,(r2._2._1, r1._2._2,r2._2._3,values_plus))
      }else if (r1._2._1 >= r2._2._1 && r1._2._2 <= r2._2._2) {
        toRet ::= (r1._1,(r1._2._1,r1._2._2,r2._2._3,values_plus))
      } else if (r2._2._1 > r1._2._1 && r2._2._2 < r1._2._2) {
        toRet::= (r1._1,(r1._2._1,r2._2._1,r1._2._3,values))
        toRet::= (r1._1,(r2._2._1,r2._2._2,r1._2._3,values_plus))
        toRet::= (r1._1,(r2._2._2,r1._2._2,r1._2._3,values))
      }else if (r2._2._1 <= r1._2._1 && r2._2._2 < r1._2._2) {
        toRet::= (r1._1,(r1._2._1,r2._2._2,r1._2._3,values_plus ))
        toRet::= (r1._1,(r2._2._2,r1._2._2,r1._2._3,values))
      }else
        toRet ::= r1
      toRet
    }
  }
}

