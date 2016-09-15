package it.polimi.genomics.spark.implementation.RegionsOperators

import com.google.common.hash.Hashing
import it.polimi.genomics.core.DataStructures.JoinParametersRD.RegionBuilder.RegionBuilder
import it.polimi.genomics.core.DataStructures.JoinParametersRD._
import it.polimi.genomics.core.DataStructures.{SomeMetaJoinOperator, OptionalMetaJoinOperator, MetaJoinOperator, RegionOperator}
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.GValue
import it.polimi.genomics.core.GRecordKey
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.collection.Map

/**
 * Created by abdulrahman kaitoua on 20/06/15.
 **/
object GenometricJoin3 {

  private final val logger = LoggerFactory.getLogger(this.getClass);

  @throws[SelectFormatException]
  def apply(executor : GMQLSparkExecutor, metajoinCondition : OptionalMetaJoinOperator, joinCondition : List[JoinQuadruple], regionBuilder : RegionBuilder, leftDataset : RegionOperator, rightDataset : RegionOperator, BINNING_PARAMETER:Long, MAXIMUM_DISTANCE:Long, sc : SparkContext) : RDD[GRECORD] = {
    logger.info("----------------Join3 executing..")

    // load datasets
    val ref : RDD[GRECORD] =
      executor.implement_rd(leftDataset, sc)
    val exp : RDD[GRECORD] =
      executor.implement_rd(rightDataset, sc)

    // load grouping
    val Bgroups: Option[Broadcast[Map[Long, Long]]] =
      if(metajoinCondition.isInstanceOf[SomeMetaJoinOperator]){ //TODO this is not right need to be fixed
        Some(sc.broadcast(executor.implement_mjd(metajoinCondition, sc).flatMap{x=>
          val hs = Hashing.md5.newHasher.putLong(x._1)
          x._2.map{ exp_id =>
            hs.putLong(exp_id)
          }
          val groupID = hs.hash().asLong()
          val e = for(ex <- x._2)
          yield(ex,groupID)
          e :+ (x._1,groupID)
        }.collectAsMap()))
      } else {
        None
      }

    // assign group to ref
    val binnedRegions: RDD[((Long, String), (Long, Long, Long, Char, Array[GValue]))] =
      assignRegionGroups( ref, Bgroups)//.repartition(10)(Ordering[Long].on(x=>x._1._1))
//   println("Count is: "+ binnedRegions.count())
    // (Expid, refID, chr, start, stop, strand, values, aggregationId)
    val start = 0;
    val stop = 999999;
    val step = 100;
    val partition = 10
//    val start = 0;
//    val stop = 10;
//    val step = 1;
//    val partition = 1
//    val partitioned = binnedRegions.repartition(partition)
//    val rdds = for(i <- (start to stop).by(step))
//      yield binnedRegions.mapPartitions({p=>p.slice(i, i+99)},preservesPartitioning = true)

//    rdds.foreach(x=>x.collect().foreach(s=>println(x.id+"/"+s)))
    // assign group and bin experiment
    val binnedExp: RDD[((Long, String), (Long,Long, Long, Char, Array[GValue]))] = //: RDD[(Long, Int, Int, Long, String, Long, Long, Char, Array[GValue])] =
      assignRegionGroups( exp, Bgroups)
    // (ExpID,chr ,bin), start, stop, strand, values,BinStart)

   val dd =  joinCondition.flatMap((q) => {
      val qList = q.toList()

      val firstRoundParameters : JoinExecutionParameter =
        createExecutionParameters(qList.takeWhile(!_.isInstanceOf[MinDistance]))
      val remaining : List[AtomicCondition] =
        qList.dropWhile(!_.isInstanceOf[MinDistance])
      val minDistanceParameter : Option[AtomicCondition] =
        remaining.headOption
      val secondRoundParameters : JoinExecutionParameter =
        createExecutionParameters(remaining.drop(1))
      //Key of join (expID, chr, bin)
      //result : aggregation,(groupid, Chr, rStart,rStop, rStrand, rValues, eStart, eStop, eStrand, eValues, Distance)

     List(binnedRegions).map{rdd=>
      val joined =  rdd.join(binnedExp)
      val firstRound: RDD[(Long, (Long, String, Long, Long, Char, Array[GValue], Long, Long, Char, Array[GValue], Long))] = // : RDD[(Long, Long, String, Long, Long, Char, Array[GValue], Long, Long, Char, Array[GValue], Long)] =
        joined .flatMap{x=> val r = x._2._1; val e = x._2._2;
          val distance : Long = distanceCalculator((r._2, r._3), (e._2, e._3))

          val same_strand = (r._4.equals('*') || e._4.equals('*') || r._4.equals(e._4))
          val intersect_distance = (!firstRoundParameters.max.isDefined || firstRoundParameters.max.get >= distance) && (!firstRoundParameters.min.isDefined || firstRoundParameters.min.get < distance)
          val no_stream = ( !firstRoundParameters.stream.isDefined )
          val UPSTREAM = if(no_stream) true else(
            firstRoundParameters.stream.get.equals('+') // upstream
              &&
              (
                ((r._4.equals('+') || r._4.equals('*')) && e._3 <= r._2) // reference with positive strand =>  experiment must be earlier
                  ||
                  ((r._4.equals('-')) && e._2 >= r._3) // reference with negative strand => experiment must be later
                )
            )
          val DOWNSTREAM = if(no_stream) true else
            (
              firstRoundParameters.stream.get.equals('-') // downstream
                &&
                (
                  ((r._4.equals('+') || r._4.equals('*')) && e._3 >= r._2) // reference with positive strand =>  experiment must be later
                    ||
                    ((r._4.equals('-')) && e._2 <= r._3) // reference with negative strand => experiment must be earlier
                  )
              )
          if(same_strand && intersect_distance && ( no_stream || UPSTREAM || DOWNSTREAM)){
            val aggregationId: Long = Hashing.md5.newHasher.putString(r._1+e._1+r._2+r._3+r._4+r._5.mkString("/"),java.nio.charset.Charset.defaultCharset()).hash().asLong
            Some(aggregationId,(Hashing.md5.newHasher.putLong(r._1).putLong(e._1).hash.asLong, x._1._2, r._2, r._3, r._4,r._5, e._2, e._3, e._4, e._5, distance))
          }else None
        }

      //  aggregation,(groupid, Chr, rStart,rStop, rStrand, rValues, eStart, eStop, eStrand, eValues, Distance)
      val minDistance: RDD[(Long, (Long, String, Long, Long, Char, Array[GValue], Long, Long, Char, Array[GValue], Long))] = //: DataSet[(Long, Long, String, Long, Long, Char, Array[GValue], Long, Long, Char, Array[GValue], Long)] =
        if (minDistanceParameter.isDefined) {
          firstRound.groupByKey()
            .flatMap(x=>x._2.toList.sortBy(_._11)(Ordering[Long]).take(minDistanceParameter.get.asInstanceOf[MinDistance].number).map(s=> (x._1,s)))
        } else {
          firstRound
        }
      val res: RDD[GRECORD] =
        if (secondRoundParameters.max.isDefined || secondRoundParameters.min.isDefined || secondRoundParameters.stream.isDefined) {
          minDistance.flatMap{p=>
            val distance = p._2._11
            if (
            // same strand or one is neutral
              (p._2._5.equals('*') || p._2._9.equals('*') || p._2._5.equals(p._2._9)) &&
                // distance
                (!secondRoundParameters.max.isDefined || secondRoundParameters.max.get >= distance) && (!secondRoundParameters.min.isDefined || secondRoundParameters.min.get < distance) &&
                // upstream downstream
                (  /*NO STREAM*/
                  ( !secondRoundParameters.stream.isDefined ) // nostream
                    ||
                    /*UPSTREAM*/
                    (
                      secondRoundParameters.stream.get.equals('+') // upstream
                        &&
                        (
                          ((p._2._5.equals('+') || p._2._5.equals('*')) && p._2._8 <= p._2._3) // reference with positive strand =>  experiment must be earlier
                            ||
                            ((p._2._5.equals('-')) && p._2._7 >= p._2._4) // reference with negative strand => experiment must be later
                          )
                      )
                    ||
                    /* DOWNSTREAM*/
                    (
                      secondRoundParameters.stream.get.equals('-') // downstream
                        &&
                        (
                          ((p._2._5.equals('+') || p._2._5.equals('*')) && p._2._7 >= p._2._4) // reference with positive strand =>  experiment must be later
                            ||
                            ((p._2._5.equals('-')) && p._2._8 <= p._2._3) // reference with negative strand => experiment must be earlier
                          )
                      )
                  )
            ) {
              val tuple = joinRegions(p, regionBuilder)
              if (tuple.isDefined) tuple else None
            } else None
          }

        } else {
          minDistance.flatMap{p =>
            val tuple = joinRegions(p, regionBuilder)
            if (tuple.isDefined){
              tuple
            }else None
          }
        }
      res
      }
    })
//      .reduce((a : RDD[GRECORD], b : RDD[GRECORD]) => {
//      a.union(b)
//    })
    sc.union(dd.toSeq)

  }


  ////////////////////////////////////////////////////
  //ref
  ////////////////////////////////////////////////////

  def assignRegionGroups(ds: RDD[GRECORD], Bgroups: Option[Broadcast[Map[Long, Long]]]): RDD[((Long,String), (Long, Long, Long, Char, Array[GValue]/*, Long*/))] = {
    ds.flatMap { region =>
      if (Bgroups.isDefined) {
      val group = Bgroups.get.value
      val expIDS = group.get(region._1._1)
      if (expIDS.isDefined) {
        val groupID = group.get(region._1._1).get
        Some((groupID,region._1._2 ),(region._1._1, region._1._3, region._1._4, region._1._5, region._2 /*, aggregationId*/))

      } else None
    }else{
        Some((1L, region._1._2),(region._1._1, region._1._3, region._1._4, region._1._5, region._2 /*, aggregationId*/))
      }
    }
  }

  def prepareDs(ds : RDD[( Long,Long, String, Long, Long, Char, Array[GValue]/*, Long*/)],BINNING_PARAMETER:Long,firstRound : JoinExecutionParameter, secondRound : JoinExecutionParameter,MAXIMUM_DISTANCE:Long) : RDD[((Long, String,Int),( Long,Long, Long, Char, Array[GValue]/*, Long*/, Int))] = {
    ds.flatMap{r  =>
      val hs = Hashing.md5.newHasher
      val maxDistance : Long =
        if(firstRound.max.isDefined) firstRound.max.get
        else if(secondRound.max.isDefined) Math.max(secondRound.max.get, MAXIMUM_DISTANCE)
        else MAXIMUM_DISTANCE
      val start1 : Long = if(!firstRound.stream.isDefined || (firstRound.stream.get.equals(r._6)) || (r._6.equals('*') && firstRound.stream.get.equals('+')) ) r._4 - maxDistance else r._5
      val end1 : Long = if(firstRound.min.isDefined) r._4 - firstRound.min.get else 0L
      val split : Boolean = firstRound.min.isDefined
      val start2 : Long = if(firstRound.min.isDefined) r._5 + firstRound.min.get else 0L
      val end2 : Long = if(!firstRound.stream.isDefined || (!firstRound.stream.get.equals(r._6)) || (r._6.equals('*') && firstRound.stream.get.equals('-')) ) r._5 + maxDistance else  r._5
      if(split){
        //(binStart, bin)
        val binPairs : List[(Int, Int)] =
          calculateBins(firstRound.stream, start1, end1, start2, end2,BINNING_PARAMETER)

        for(p <- binPairs)
        yield((r._1 ,r._3,p._2),(r._2, r._4, r._5, r._6, r._7/*, r._8*/, p._1))

      } else {

        val binStart = if ( (start1 / BINNING_PARAMETER).toInt < 0 ) 0 else (start1 / BINNING_PARAMETER).toInt
        val binEnd = (end2 / BINNING_PARAMETER).toInt
        for (i <- binStart to binEnd)
        yield((r._1,r._3,i),(r._2, r._4, r._5, r._6, r._7/*, r._8*/, binStart) )
      }
    }
  }

  def calculateBins(stream : Option[Char], start1 : Long, end1 : Long, start2 : Long, end2 : Long,BINNING_PARAMETER:Long) : List[(Int, Int)] ={

    val a  = // TODO should check the Strand too for upstream and downstream
//      if(!stream.isDefined || stream.get.equals('-')){
      if(end1 > start1){
        val binStart1 = if ( (start1 / BINNING_PARAMETER).toInt < 0 ) 0 else (start1 / BINNING_PARAMETER).toInt
        val binEnd1 = if ( (end1 / BINNING_PARAMETER).toInt < 0 ) 0 else (end1 / BINNING_PARAMETER).toInt
        (binStart1 to binEnd1).map((v) => (binStart1, v))
        //          for (i <- binStart1 to binEnd1)
        //            yield((binStart1, i))

      } else {
        List()
      }

    val b =
//      if(!stream.isDefined || stream.get.equals('+')){
      if(end2 > start2){
        val binStart2 = (start2 / BINNING_PARAMETER).toInt
        val binEnd2 = (end2 / BINNING_PARAMETER).toInt
        (binStart2 to binEnd2).map((v) => (binStart2, v))
        //          for (i <- binStart2 to binEnd2)
        //            yield((binStart2, i))
      } else {
        List()
      }

    (a ++ b).toList
  }

  def binExperiment(ds: RDD[GRECORD], Bgroups:  Option[Broadcast[Map[Long, Long]]],BINNING_PARAMETER:Long): RDD[((Long, String, Int), ( Long,Long, Long, Char, Array[GValue], Int))] = {
    // : RDD[(Long, Int, Long, String, Long, Long, Char, Array[GValue])] = {
    //assignExperimentGroups(executor : FlinkImplementation, ds: DataSet[FlinkRegionType], groups: DataSet[(Long, Long)], env : ExecutionEnvironment): DataSet[(Long, Int, Int, Long, String, Long, Long, Char, Array[GValue])] = {

    ds.flatMap { region =>
      val binStart = (region._1._3 / BINNING_PARAMETER).toInt
      val binEnd = (region._1._4 / BINNING_PARAMETER).toInt
      for (i <- binStart to binEnd)
      yield if (Bgroups.isDefined) {
        val group = Bgroups.get.value
        (((group.get(region._1._1).get, region._1._2, i), (region._1._1, region._1._3, region._1._4, region._1._5, region._2, binStart)))
      } else {
        (((1L, region._1._2, i), (region._1._1, region._1._3, region._1._4, region._1._5, region._2, binStart)))
      }
    }
  }

  def joinRegions(p : (Long, (Long, String, Long, Long, Char, Array[GValue], Long, Long, Char, Array[GValue], Long)), regionBuilder : RegionBuilder) : Option[GRECORD] = {
    regionBuilder match {
      case RegionBuilder.LEFT => Some(new GRecordKey(p._2._1, p._2._2, p._2._3, p._2._4, p._2._5),  p._2._6 ++ p._2._10)
      case RegionBuilder.RIGHT => Some(new GRecordKey(p._2._1, p._2._2, p._2._7, p._2._8,  p._2._9),p._2._6 ++ p._2._10)
      case RegionBuilder.INTERSECTION => joinRegionsIntersection(p)
      case RegionBuilder.CONTIG =>
        Some(new GRecordKey(p._2._1, p._2._2, Math.min(p._2._3, p._2._7), Math.max(p._2._4, p._2._8), if(p._2._5.equals(p._2._9)) p._2._5 else '*'), p._2._6 ++ p._2._10)
    }
  }

  def joinRegionsIntersection(p : (Long, (Long, String, Long, Long, Char, Array[GValue], Long, Long, Char, Array[GValue], Long))) : Option[GRECORD] = {
    if(p._2._3 < p._2._8 && p._2._4 > p._2._7) {
      val start: Long = Math.max(p._2._3, p._2._7)
      val stop : Long = Math.min(p._2._4, p._2._8)
      val strand: Char = if (p._2._5.equals(p._2._9)) p._2._5 else '*'
      val values: Array[GValue] = p._2._6 ++ p._2._10
      Some(new GRecordKey(p._2._1, p._2._2, start, stop, strand), values)
    } else {
      None
    }
  }

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

  def createExecutionParameters(list : List[AtomicCondition]) : JoinExecutionParameter = {
    def helper(list : List[AtomicCondition], temp : JoinExecutionParameter) : JoinExecutionParameter = {
      if(list.size.equals(0)){
        temp
      } else {
        val current = list.head
        current match{
          case DistLess(v) => helper(list.tail, new JoinExecutionParameter(Some(v), temp.min, temp.stream))
          case DistGreater(v) => helper(list.tail, new JoinExecutionParameter(temp.max, Some(v), temp.stream))
          case Upstream() => helper(list.tail, new JoinExecutionParameter(temp.max, temp.min, Some('+')))
          case DownStream() => helper(list.tail, new JoinExecutionParameter(temp.max, temp.min, Some('-')))
        }
      }
    }

    helper(list, new JoinExecutionParameter(None, None, None))
  }

  class JoinExecutionParameter(val max : Option[Long], val min : Option[Long], val stream : Option[Char]) extends Serializable {
    override def toString() = {
      "JoinParam max:" + {
        if (max.isDefined) {
          max.get
        }
      } +  " min: " + {
        if (min.isDefined) {
          min.get
        }
      } + " stream: " + {
        if (stream.isDefined) {
          stream.get
        }
      }
    }
  }


}

