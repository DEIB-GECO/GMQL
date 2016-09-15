package it.polimi.genomics.spark.implementation.RegionsOperators.GenometricMap

import com.google.common.hash.Hashing
import it.polimi.genomics.core.DataStructures.JoinParametersRD.RegionBuilder
import it.polimi.genomics.core.DataStructures.JoinParametersRD.RegionBuilder._
import it.polimi.genomics.core.DataStructures.RegionAggregate.RegionsToRegion
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.GRecordKey
import it.polimi.genomics.spark.implementation.RegionsOperators.GenometricJoin2.JoinExecutionParameter
import org.apache.spark.rdd.RDD
import scala.collection.immutable.Queue
import org.slf4j.LoggerFactory
import it.polimi.genomics.core.{GValue, GString, GDouble}

/**
 * Created by Abdulrahman Kaitoua on 08/06/15.
 * Email: abdulrahman.kaitoua@polimi.it
 *
 */
object GMAP{
  implicit class ChromRDD(rdd: RDD[((Long,String), Iterable[GRECORD])]) {
    final val logger = LoggerFactory.getLogger(this.getClass)

    def GMap(Ref_broadcast: Array[(Int,Map[String,Array[GRECORD]])],
             offset_l: Int, offset_r: Int,outOp: List[RegionsToRegion],bin:Int): RDD[((Long,String), Iterable[GRECORD])] = {
      logger.info("ScanMap is called :) \n\n")

      rdd.flatMap { exp_chrom =>
        val e_chrom = if (exp_chrom._1._2.indexOf("_") > 0) exp_chrom._1._2.substring(0, exp_chrom._1._2.indexOf("_")) else exp_chrom._1._2
        val iExp: Iterator[GRECORD] = exp_chrom._2.iterator;

        if(iExp.hasNext)
          Ref_broadcast.map { ref_sample =>
            val ref_Chrom = ref_sample._2.get(exp_chrom._1._2) match {
              case Some(x) => x.iterator
              case None => Iterator.empty; //TODO throw exception to pass this sample (does not contain this chrom)
            }

            val newID = Hashing.murmur3_128().newHasher().putLong(exp_chrom._1._1).putLong(ref_sample._1).hash().asLong()
            val chrom_out = scanMapChroms(ref_Chrom, iExp, outOp, offset_l, offset_r,bin)
            ((newID,e_chrom), chrom_out.toIterable)
          }
        else
        {
          logger.warn(s"Empty Exp!!! ${exp_chrom._1}")
          None
        }
      }
    }.setName("Genomic Map on Chromosomes")

    def ids(): Array[Long]= rdd.values.map(x=>x.iterator.next()._1._1).distinct().collect();


  }

  //  def JoinMinDistCondition(ref:GRECORD, R:GRECORD,dist:Int, minDist:Int)(implicit offset:Offsets):Boolean={
  //    return true;
  //  }

  class FiniteQueue[A](q: Queue[A]) {
    def enqueueFinite[B >: A](elem: B, maxSize: Int): Queue[B] = {
      var ret = q.enqueue(elem)
      while (ret.size > maxSize) { ret = ret.dequeue._2 }
      ret
    }
  }
  implicit def queue2finitequeue[A](q: Queue[A]) = new FiniteQueue[A](q)

  def distanceCalculator(a : (Long, Long), b : (Long, Long)) : Long = {
    // b to right of a
    if(b._1 >= a._2){
      b._1 - a._2
    } else if(b._2 <= a._1) a._1 - b._2
    else {
      // intersecting
      Math.max(a._1, b._1) - Math.min(a._2, b._2)
    }

  }
  def strandCompatible(r:Char,e:Char) = r.equals('*') || e.equals('*') || r.equals(e)

  def scanMapChroms(ref_regions:Iterator[GRECORD],iExp:Iterator[GRECORD],
                    outOp:List[RegionsToRegion]
                    , offset_l:Int, offset_r:Int, bin:Int ):Iterator[GRECORD] = {//Iterator[(GRECORD, List[List[DataTypes.GValue]])] = {

    //init empty list for caching regions
    var RegionCache= List[GRECORD]();
    var temp = List[GRECORD]() ;
    var intersectings = List[GRECORD]();
    //logger.info(iExp.size.toString+"\t"+ref_regions.size.toString)

    var exp_region:GRECORD = (new GRecordKey(),Array[GValue]())
    //    if(iExp.hasNext)
    exp_region = iExp.next;
    val expId = exp_region._1._1
    //    else
    //      logger.warn(s"Experiment got empty while it was not !!!")

    val mD = Math.max(offset_l, offset_r);
    //println(ref_regions.size)
    ref_regions.flatMap(ref_region => {
      //      try{
      var _distL = offset_l;
      var _distR = offset_r;
      if (ref_region._1._5.equals('-')) {
        _distL = offset_r;
        _distR = offset_l;
      }
      //clear the intersection list
      intersectings = List.empty;
      temp = List.empty;

      //check the cache
      RegionCache.map(cRegion=> {
        if (distanceLessThan(ref_region, cRegion, mD, mD)) {
          // print("intersects ")
          if (sameBin(ref_region, cRegion,bin)&&strandCompatible(ref_region, cRegion) && distanceLessThan(ref_region, cRegion, _distL, _distR))
          {
            intersectings ::=cRegion;
          }
          temp ::= cRegion;
        } else if (!(cRegion._1._4 < ref_region._1._3)) {
          temp ::= cRegion;
        }
      }
      )
      RegionCache = temp;

      //iterate on exp regions. Break when no intersection
      //is found or when we overcome the current reference
      while (exp_region != null && ref_region._1._4 + mD > exp_region._1._3) {
        if (distanceLessThan(ref_region, exp_region, mD, mD)) {
          //the region is inside, we process it
          if (sameBin(ref_region, exp_region,bin)&&strandCompatible(ref_region, exp_region) && distanceLessThan(ref_region, exp_region, _distL, _distR))
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
      // val GValuesLists= outOp.map{x=>val gvalueList = intersectings.map(inter => inter._2(x.index)).toList;gvalueList}
      val newGValues= outOp.map{x=>val gvalueList = intersectings.map(inter => inter._2(x.index)).toList;x.fun(gvalueList)}.toArray
      // println ("New Gvalues: "+newGValues.mkString(","))
      val newID = Hashing.md5().newHasher().putLong(ref_region._1._1).putLong(expId).hash().asLong()
      Some((new GRecordKey(newID,ref_region._1._2,ref_region._1._3,ref_region._1._4,ref_region._1._5),ref_region._2++ newGValues))
      // Some((new GRecordKey(newID,ref_region._1._2,ref_region._1._3,ref_region._1._4,ref_region._1._5),ref_region._2),GValuesLists)
      //      Some(outOp(intersectings, ref_region))
    })
  }

  def  strandCompatible(r1:GRECORD, r2:GRECORD):Boolean={
    strandCompatible(r1._1,r2._1)
  }
  def  strandCompatible(r1:GRecordKey, r2:GRecordKey):Boolean={
    if(r1._5.equals(r2._5))
      true;
    else if(r1._5.equals('*') || r2._5.equals('*')
      ||r1._5.equals('.') || r2._5.equals('.'))
      true;
    else false;
  }

  def distanceLessThan( r1:GRECORD, r2:GRECORD, offset_l:Int, offset_r:Int) :Boolean ={
    distanceLessThan(r1._1,r2._1,offset_l,offset_r)
  }

  def distanceLessThan( r1:GRecordKey, r2:GRecordKey, offset_l:Int, offset_r:Int) :Boolean ={
    val t1_left = r1._3 ;
    val t1_right = r1._4;
    val t2_left = r2._3;
    val t2_right = r2._4;
    //return (t1_left <= t2_right && t1_right >= t2_left);
    return (t1_left < t2_right && t1_right > t2_left);
  }

  def sameBin(r1:GRECORD, r2:GRECORD,bin:Int) : Boolean={
    if(bin>0){
      val startr1 = Math.ceil((r1._1._3 +1) / bin)
      val stopr1 = Math.ceil((r1._1._4 +1) / bin)
      val startr2 = Math.ceil((r2._1._3 +1) / bin)
      val stopr2 = Math.ceil((r2._1._4 +1) / bin)
      if(startr1 !=stopr1 && startr2 !=stopr2 && startr1 ==startr2){
        val currentbin = getBin(r1)
        currentbin == startr1 && currentbin == startr2
      }
      else true
    }else true
  }

  def getBin(r1:GRECORD): Int ={
    val s = r1._1._2.substring(r1._1._2.indexOf('_')+1,r1._1._2.size)
    if(s.indexOf('_')>0)
      s.substring(0,s.indexOf('_')).toInt
    else
      s.toInt
  }
  final type JoinRegion = (Long,Long,Long,Char,Array[GValue],Int)

  def scanJoinChroms(key : (Long,String,Int),
                     ref_regions:Iterable[JoinRegion],
                     iExp:Iterable[JoinRegion],
                     regionBuilder : RegionBuilder,
                     RoundParameters : JoinExecutionParameter,
                     count:Int):Iterator[(Long,(GRecordKey,Array[GValue],Long))] = {
    //init empty list for caching regions

    var RegionCachePOS= List[JoinRegion]();
    var RegionCacheNEG = List[JoinRegion]()
    var pastRegions = List[JoinRegion]()
    var temp = List[(Long,JoinRegion)]() ;
    var result = List[(Long,JoinRegion)]();
    try {
      if(iExp.isEmpty) throw new Exception("Empty Experiment Bin (chr: "+"key._2"+" , Bin: "+"key._3"+" )....")
//      println ("inside the MinDistance")
      val experiments = iExp.toIterator
      val references = ref_regions.toIterator

      //    iExp.foreach(println(_))
      //    ref_regions.foreach(println (_))
      //extract the first ;count; element from the exp to be in the first result
      def addTo(i: Int,it:Iterator[JoinRegion],ref:JoinRegion):List[(Long,JoinRegion)] =  {
//        println("Creating the first result")
        if (i < count && it.hasNext) {
          val exp = it.next;
          addTo(i+1,it,ref):+(distanceCalculator((ref._2,ref._3),(exp._2,exp._3)),exp)
        }
        else List[(Long,JoinRegion)]()
      }

      result = result ::: addTo(0,experiments,ref_regions.iterator.next)

      RegionCachePOS = result.map(_._2)
      RegionCacheNEG = RegionCachePOS
      var exp_region = experiments.next;
      var minDistPOS= Long.MaxValue;
      var minDistNEG= Long.MaxValue;


      references.flatMap{ref_region =>


//        println("ref region : "+ref_region)
        // calculate the distance from the reference to all the cached regions and take the nearest ;count; region

          val cache =
            if (ref_region._4.equals('+')) RegionCachePOS
            else if (ref_region._4.equals('-')) RegionCacheNEG
            else (RegionCachePOS ::: RegionCacheNEG)
          result = cache.flatMap { cRegion =>

            val first_match = ((cRegion._6.equals(key._3)) || (ref_region._6.equals(key._3)))
            val no_stream = (!RoundParameters.stream.isDefined)
            if (first_match && (no_stream ||
              DOWNSTREAM(no_stream, ref_region, cRegion, RoundParameters) ||
              UPSTREAM(no_stream, ref_region, cRegion, RoundParameters))) {
              Some(distanceCalculator((ref_region._2, ref_region._3), (cRegion._2, cRegion._3)), cRegion)
            } else None
          }.sortBy(_._1).take(count)
//          result.foreach(x=>println ("the res "+x))
          var maxDist = result.maxBy(_._1)._1
          // look for word and keep ;count; nearest from back
          // keep the nearest ;count; regions that ends before the start of the reference
          while (exp_region != null && maxDist != Long.MinValue) {
            val first_match = ((exp_region._6.equals(key._3)) || (ref_region._6.equals(key._3)))
            val no_stream = (!RoundParameters.stream.isDefined)
            if (first_match &&
              (no_stream || DOWNSTREAM(no_stream, ref_region, exp_region, RoundParameters) ||
                UPSTREAM(no_stream, ref_region, exp_region, RoundParameters))) {
              val dist = distanceCalculator((ref_region._2, ref_region._3), (exp_region._2, exp_region._3));
              maxDist = result.maxBy(_._1)._1
//              println (ref_region,exp_region,"dist:"+dist,"maxDist"+maxDist)
              if (dist < maxDist) {
                result ::=(dist, exp_region)
                result = result.sortBy(_._1).init
              } else if (result.size < count) result ::=(dist, exp_region)
              else maxDist = Long.MinValue

              // add and remove regions from the Cache
              if (ref_region._4.equals('+') && exp_region._4.equals('+')) {
                RegionCachePOS ::= exp_region
                if (exp_region._3 < ref_region._2)
                  RegionCachePOS = RegionCachePOS.sortBy(_._3)(Ordering[Long].reverse).init
              }
              else if (ref_region._4.equals('-') && exp_region._4.equals('-')) {
                RegionCacheNEG ::= exp_region
                if (exp_region._3 < ref_region._2)
                  RegionCacheNEG = RegionCacheNEG.sortBy(_._3)(Ordering[Long].reverse).init
              }
              else if (ref_region._4.equals('*')) {
                RegionCachePOS ::= exp_region
                RegionCacheNEG ::= exp_region
                if (exp_region._3 < ref_region._2) {
                  RegionCachePOS = RegionCachePOS.sortBy(_._3)(Ordering[Long].reverse).init
                  RegionCacheNEG = RegionCacheNEG.sortBy(_._3)(Ordering[Long].reverse).init
                }
              }
            }

            if (experiments.hasNext) {
              // add to cache
              exp_region = experiments.next();
            } else {
              exp_region = null;
            }

          } // end while on exp regions

        result.flatMap(exp => joinRegions(ref_region,exp._2, key,exp._1,regionBuilder))

    }
    }catch{
      case e:Exception => /*println(e.getMessage);*/List[(Long,(GRecordKey,Array[GValue],Long))]().iterator;
    }
  }

  def UPSTREAM (no_stream:Boolean,ref_region:JoinRegion,exp_region:JoinRegion,firstRoundParameters:JoinExecutionParameter)= if(no_stream) true else(
    firstRoundParameters.stream.get.equals('+') // upstream
      &&
      (
        ((ref_region._4.equals('+') || ref_region._4.equals('*')) && exp_region._3 <= ref_region._2) // reference with positive strand =>  experiment must be earlier
          ||
          ((ref_region._4.equals('-')) && exp_region._2 >= ref_region._3) // reference with negative strand => experiment must be later
        )
    )
  def DOWNSTREAM(no_stream:Boolean,ref_region:JoinRegion,exp_region:JoinRegion,firstRoundParameters:JoinExecutionParameter) = if(no_stream) true else
    (
      firstRoundParameters.stream.get.equals('-') // downstream
        &&
        (
          ((ref_region._4.equals('+') || ref_region._4.equals('*')) && exp_region._3 >= ref_region._2) // reference with positive strand =>  experiment must be later
            ||
            ((ref_region._4.equals('-')) && exp_region._2 <= ref_region._3) // reference with negative strand => experiment must be earlier
          )
      )
  def joinRegions(ref:JoinRegion, exp:JoinRegion, key:(Long,String,Int),dist:Long /*: ( (Long, String, Long, Long, Char, Array[GValue], Long, Long, Char, Array[GValue], Long))*/, regionBuilder : RegionBuilder): Option[(Long, (GRecordKey, Array[GValue], Long))] = {
    val groupID = Hashing.md5.newHasher.putLong(ref._1).putLong(exp._1).hash.asLong
    val agg = Hashing.md5.newHasher.putString(groupID+key._2+ref._2+ ref._3+ ref._4+ref._5.mkString("/"),java.nio.charset.Charset.defaultCharset()).hash.asLong
      regionBuilder match {
      case RegionBuilder.LEFT => Some(agg,(new GRecordKey(groupID, key._2,ref._2, ref._3, ref._4),  ref._5 ++ exp._5,dist))
      case RegionBuilder.RIGHT => Some(agg,(new GRecordKey(groupID, key._2, exp._2, exp._3,  exp._4),ref._5 ++ exp._5,dist))
      case RegionBuilder.INTERSECTION => joinRegionsIntersection(ref,exp,(groupID,key._2,key._3,agg),dist)
      case RegionBuilder.CONTIG =>
        Some(agg,(new GRecordKey(groupID, key._2, Math.min(ref._2, exp._2), Math.max(ref._3, exp._3), if(ref._4.equals(exp._4)) ref._4 else '*'), ref._5 ++ exp._5,dist))
    }
  }

  def joinRegionsIntersection(ref:JoinRegion, exp:JoinRegion, key:(Long,String,Int,Long),dist:Long): Option[(Long, (GRecordKey, Array[GValue], Long))] = {
    if(ref._2 < exp._3 && ref._3 > exp._2) {
      val start: Long = Math.max(ref._2, exp._2)
      val stop : Long = Math.min(ref._3, exp._3)
      val strand: Char = if (ref._4.equals(exp._4)) exp._4 else '*'
      val values: Array[GValue] = ref._5 ++ exp._5
      Some(key._4,(new GRecordKey(key._1, key._2, start, stop, strand), values,dist))
    } else {
      None
    }
  }
}