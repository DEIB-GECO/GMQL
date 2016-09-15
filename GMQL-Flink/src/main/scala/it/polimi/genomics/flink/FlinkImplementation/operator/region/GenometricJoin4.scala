package it.polimi.genomics.flink.FlinkImplementation.operator.region

import it.polimi.genomics.core.DataStructures.JoinParametersRD.AtomicCondition
import it.polimi.genomics.core.DataStructures.JoinParametersRD.DistGreater
import it.polimi.genomics.core.DataStructures.JoinParametersRD.DistLess
import it.polimi.genomics.core.DataStructures.JoinParametersRD.DownStream
import it.polimi.genomics.core.DataStructures.JoinParametersRD.JoinQuadruple
import it.polimi.genomics.core.DataStructures.JoinParametersRD.MinDistance
import it.polimi.genomics.core.DataStructures.JoinParametersRD.RegionBuilder.RegionBuilder
import it.polimi.genomics.core.DataStructures.JoinParametersRD.Upstream
import it.polimi.genomics.core.DataStructures.JoinParametersRD._
import it.polimi.genomics.core.DataStructures.{SomeMetaJoinOperator, OptionalMetaJoinOperator, MetaJoinOperator, RegionOperator}
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.GValue
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.flink.FlinkImplementation.FlinkImplementation
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._
import org.apache.flink.hadoop.shaded.com.google.common.base.Charsets
import org.apache.flink.hadoop.shaded.com.google.common.hash.Hashing
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory
import scala.annotation.tailrec
import scala.collection.immutable.{HashSet}

object GenometricJoin4 {
  final val logger = LoggerFactory.getLogger(this.getClass)

  @throws[SelectFormatException]
  def apply(executor : FlinkImplementation, metajoinCondition : OptionalMetaJoinOperator, joinCondition : List[JoinQuadruple], regionBuilder : RegionBuilder, leftDataset : RegionOperator, rightDataset : RegionOperator, env : ExecutionEnvironment, binSize : Long, maximumDistance : Long) : DataSet[FlinkRegionType] = {

    // load datasets
    val ds : DataSet[FlinkRegionType] =
      executor.implement_rd(leftDataset, env)
    val exp : DataSet[FlinkRegionType] =
      executor.implement_rd(rightDataset, env)

    //creating the groups if they exist
    //otherwise create all possible (ref,exp) pairs
    val groups : Option[HashSet[(Long, Long)]] =
      if(metajoinCondition.isInstanceOf[SomeMetaJoinOperator]){
        val groups : DataSet[FlinkMetaJoinType] =
          executor.implement_mjd3(metajoinCondition, env)

        Some(
          groups
            .join(groups).where(1,2).equalTo(1,3){
            (a, b, out: Collector[HashSet[(Long, Long)]]) => {
              //if (!a._1.equals(b._1)) {
              out.collect(HashSet[(Long, Long)]((a._1, b._1)))
              //}
            }
          }
            .reduce(_ ++ _)
            .collect()
            .head
        )
      } else {
        None
      }

    //(sampleID, sampleID)

    ////////////////////////////////////////////////////
    // assign group to ref
    ////////////////////////////////////////////////////

    //val groupedDs : DataSet[(Long, Long, String, Long, Long, Char, Array[GValue], Long)] =
    //  assignAnchorGroups(executor, ds, groups, env)
    // (newId, expId, oldSample, chr, start, stop, strand, values, aggregationId)

    ////////////////////////////////////////////////////
    //assign group and bin experiment
    ////////////////////////////////////////////////////

    val binnedExp : DataSet[(Int, Long, String, Long, Long, Char, Array[GValue])] =
      binExperiment(exp, binSize)

    joinCondition.map((q) => {
      val qList = q.toList()

      ////////////////////////////////////////////////////
      //prepare three blocks of join conditions
      ////////////////////////////////////////////////////
      val firstRoundParameters : JoinExecutionParameter =
        createExecutionParameters(qList.takeWhile(!_.isInstanceOf[MinDistance]))
      val remaining : List[AtomicCondition] =
        qList.dropWhile(!_.isInstanceOf[MinDistance])
      val minDistanceParameter : Option[AtomicCondition] =
        remaining.headOption
      val secondRoundParameters : JoinExecutionParameter =
        createExecutionParameters(remaining.drop(1))


      val maxDistance : Long =
        if(firstRoundParameters.max.isDefined) firstRoundParameters.max.get
        else if(secondRoundParameters.max.isDefined) Math.max(secondRoundParameters.max.get, maximumDistance)
        else maximumDistance

      val minimunDistance : Option[Long] =
        if(firstRoundParameters.min.isDefined) Some(firstRoundParameters.min.get)
        else if(secondRoundParameters.min.isDefined) Some(secondRoundParameters.min.get)
        else None

      val upstream_shift = if (!firstRoundParameters.stream.isDefined || firstRoundParameters.stream.get == '+')
          maxDistance
        else
          0

      val downstream_shift = if (!firstRoundParameters.stream.isDefined || firstRoundParameters.stream.get == '-')
          maxDistance
        else
          0

      val first_round_join_condition = FirstRoundCondition(build_condition(firstRoundParameters.min,maxDistance,firstRoundParameters.stream))
      val first_round_bin_condition = new FirstRoundBinCondition(binSize,upstream_shift,downstream_shift)

      if (minDistanceParameter.isDefined) {
        val binnedAnchor = binAnchorMinDistance(ds, binSize, upstream_shift, downstream_shift)

        val firstRound = binnedAnchor.join(binnedExp).where(3,0).equalTo(2,0){
          (a,e,out : Collector[(Long, Long, String, Long, Long, Char, Array[GValue], Long, Long, Char, Array[GValue], Long, Int)]) => {

            if (first_round_bin_condition.f(a._5, a._7, e._4, a._1) && ( groups.isEmpty || groups.get.contains((a._3, e._2)))) {
              val optional_distance : Option[Long] = first_round_join_condition.f(a._5,a._6, a._7, e._4, e._5)
              if (optional_distance.isDefined) {
                val hashId : Long = Hashing.md5().newHasher().putLong(a._3).putLong(e._2).hash().asLong()
                out.collect((hashId, a._2, a._4, a._5, a._6, a._7, a._8, e._4,e._5,e._6,e._7,optional_distance.get,a._1))
              }
            }
          }
        }.withForwardedFieldsFirst("1->1;7->6;0->12;3->2;4->3;5->4;6->5")
          .withForwardedFieldsSecond("6->10;3->7;4->8;5->9")

        val minDistance =
            firstRound
              .groupBy(2, 12, 0, 1)
              .sortGroup(11 , Order.ASCENDING)
              .reduceGroup((i : Iterator[(Long, Long, String, Long, Long, Char, Array[GValue], Long, Long, Char, Array[GValue], Long, Int)], out : Collector[(Long, Long, String, Long, Long, Char, Array[GValue], Long, Long, Char, Array[GValue], Long, Int)]) => {
                minDistanceSelector(i, minDistanceParameter.get.asInstanceOf[MinDistance].number, Long.MinValue, out)
              }).withForwardedFields("0;1;2;3;4;5;7;8;9;11;12")
              //firstRound
              .groupBy(0,1)
              .sortGroup(11 , Order.ASCENDING)
              .reduceGroup((i : Iterator[(Long, Long, String, Long, Long, Char, Array[GValue], Long, Long, Char, Array[GValue], Long, Int)], out : Collector[(Long, Long, String, Long, Long, Char, Array[GValue], Long, Long, Char, Array[GValue], Long, Int)]) => {
                minDistanceSelector(i, minDistanceParameter.get.asInstanceOf[MinDistance].number, Long.MinValue, out)
              }).withForwardedFields("0;1;2;3;4;5;7;8;9;11;12")

        val res: DataSet[FlinkRegionType] =
          if (secondRoundParameters.max.isDefined || secondRoundParameters.min.isDefined || secondRoundParameters.stream.isDefined) {
            minDistance.flatMap((p, out: Collector[FlinkRegionType]) => {
              val distance = p._12
              if (
              // same strand or one is neutral
                (p._6.equals('*') || p._10.equals('*') || p._6.equals(p._10)) &&
                  // distance
                  (secondRoundParameters.max.isEmpty || secondRoundParameters.max.get > distance) && (secondRoundParameters.min.isEmpty || secondRoundParameters.min.get < distance) &&
                  // upstream downstream
                  ( /* NO STREAM  */
                    ( secondRoundParameters.stream.isEmpty ) // nostream
                      ||
                      /*  UPSTREAM  */
                      (
                        secondRoundParameters.stream.get.equals('+') // upstream
                          &&
                          (
                            ((p._6.equals('+') || p._6.equals('*')) && p._9 <= p._4) // reference with positive strand =>  experiment must be earlier
                              ||
                              ((p._6.equals('-')) && p._8 >= p._5) // reference with negative strand => experiment must be later
                            )
                        )
                      ||
                      /* DOWNSTREAM */
                      (
                        secondRoundParameters.stream.get.equals('-') // downstream
                          &&
                          (
                            ((p._6.equals('+') || p._6.equals('*')) && p._8 >= p._5) // reference with positive strand =>  experiment must be later
                              ||
                              ((p._6.equals('-')) && p._9 <= p._4) // reference with negative strand => experiment must be earlier
                            )
                        )
                    )
              ) {
                val tuple = joinRegionsMinDistance(p, regionBuilder)
                if (tuple.isDefined){
                  out.collect(tuple.get)
                }
              }
            })
          } else {
            minDistance.flatMap((p, out : Collector[FlinkRegionType]) => {
              val tuple = joinRegionsMinDistance(p, regionBuilder)
              if (tuple.isDefined){
                out.collect(tuple.get)
              }
            })
          }
          res


      } else {

        val binnedAnchor = binAnchor(ds, binSize, upstream_shift, downstream_shift)

        val firstRound = binnedAnchor.join(binnedExp).where(2,0).equalTo(2,0) {
          (a,e, out:Collector[FlinkRegionType]) => {
            if (first_round_bin_condition.f(a._4, a._6, e._4, a._1) && ( groups.isEmpty || groups.get.contains((a._2, e._2)))) {
              val optional_distance : Option[Long] = first_round_join_condition.f(a._4,a._5, a._6, e._4, e._5)
              if (optional_distance.isDefined) {
                val hashId : Long = Hashing.md5().newHasher().putLong(a._2).putLong(e._2).hash().asLong()
                val p = (hashId,a._3,a._4,a._5,a._6,a._7,e._4,e._5,e._6,e._7, optional_distance.get)
                val tuple = joinRegions(p, regionBuilder)
                if (tuple.isDefined){
                  out.collect(tuple.get)
                }
              }
            }

          }
        }

        firstRound

      }
    })
      ////////////////////////////////////////////////////
      // Union of all join quadruple results
      ////////////////////////////////////////////////////
      .reduce((a : DataSet[FlinkRegionType], b : DataSet[FlinkRegionType]) => {
      a.union(b)
    })
  }


  ////////////////////////////////////////////////////
  //experiment (sample)
  ////////////////////////////////////////////////////

  def binAnchor(ds: DataSet[FlinkRegionType], binSize : Long,
                upstream_shift : Long, downstream_shift : Long) = {
    ds.flatMap((r : (Long, String, Long, Long, Char, Array[GValue])) => {
      val (left_shift,rigth_shift) =
        if (r._5 == '*' || r._5 == '+') (upstream_shift, downstream_shift) else (downstream_shift,upstream_shift)
      val first_bin = ((r._3 - left_shift) / binSize)
      val last_bin = ((r._4 + rigth_shift) / binSize)

      for(bin <- Math.max(first_bin,0) to last_bin) yield (bin.toInt, r._1,r._2,r._3,r._4,r._5,r._6)
    })

  }

  def binAnchorMinDistance(ds: DataSet[FlinkRegionType], binSize : Long,
                upstream_shift : Long, downstream_shift : Long) = {
    ds.flatMap((r : (Long, String, Long, Long, Char, Array[GValue])) => {
      val (left_shift,rigth_shift) =
        if (r._5 == '*' || r._5 == '+') (upstream_shift, downstream_shift) else (downstream_shift,upstream_shift)
      val first_bin = ((r._3 - left_shift) / binSize)
      val last_bin = ((r._4 + rigth_shift) / binSize)

      val aggregationId: Long =
        Hashing.md5().newHasher()
          .putLong(r._1)
          .putString(r._2, Charsets.UTF_8)
          .putLong(r._3)
          .putLong(r._4)
          .putChar(r._5)
          .putString(r._6.mkString("§"), Charsets.UTF_8)
          .hash().asLong()
      for(bin <- Math.max(first_bin,0) to last_bin) yield (bin.toInt, aggregationId, r._1,r._2,r._3,r._4,r._5,r._6)
    })
  }

  /*returns: (bin,id,chr,start,stop,strand,values)*/
  def binExperiment(ds: DataSet[FlinkRegionType], binSize : Long): DataSet[(Int, Long, String, Long, Long, Char, Array[GValue])] = {
    ds.flatMap((region : FlinkRegionType, out : Collector[(Int, Long, String, Long, Long, Char, Array[GValue])]) => {
      val binStart = (region._3 / binSize).toInt
      val binEnd = (region._4 / binSize).toInt
      for (bin <- binStart to binEnd) {
        out.collect((bin, region._1, region._2, region._3, region._4, region._5, region._6))
      }
    })
  }
  //(binStart, bin, id, chr, start, stop, strand, values)

  ////////////////////////////////////////////////////
  //mindistance
  ////////////////////////////////////////////////////
  @tailrec
  def minDistanceSelector(i : Iterator[(Long, Long, String, Long, Long, Char, Array[GValue], Long, Long, Char, Array[GValue], Long, Int)], currentCount : Int, currentValue : Long, out : Collector[(Long, Long, String, Long, Long, Char, Array[GValue], Long, Long, Char, Array[GValue], Long, Int)]) : Unit = {
    if(i.hasNext){
      val current = i.next()
      //println((current._1, current._2, current._3, current._4, current._5, current._6, current._7(0), current._8, current._9, current._10, current._11(0), current._12))
      if(currentCount > 0 || currentValue == current._12){
        out.collect(current)
        minDistanceSelector(i, currentCount - 1, current._12, out)
      }
    }
  }


  ////////////////////////////////////////////////////
  //builders
  ////////////////////////////////////////////////////

  // (._1  , ._2  , ._3, ._4   , ._5  , ._6    , ._7    , ._8   , ._9  , ._10   , ._11   , ._12    )
  // (newID, AggId, chr, rStart, rStop, rStrand, rValues, eStart, eStop, eStrand, eValues, distance)
  def joinRegionsMinDistance(p : (Long, Long, String, Long, Long, Char, Array[GValue], Long, Long, Char, Array[GValue], Long, Int), regionBuilder : RegionBuilder) : Option[FlinkRegionType] = {
    regionBuilder match {
      case RegionBuilder.LEFT => joinRegionsLeftMinDistance(p)
      case RegionBuilder.RIGHT => joinRegionsRightMinDistance(p)
      case RegionBuilder.INTERSECTION => joinRegionsIntersectionMinDistance(p)
      case RegionBuilder.CONTIG => joinRegionsContigMinDistance(p)
    }
  }

  def joinRegionsLeftMinDistance(p : (Long, Long, String, Long, Long, Char, Array[GValue], Long, Long, Char, Array[GValue], Long, Int)) : Option[FlinkRegionType] = {
    val start : Long = p._4
    val stop : Long = p._5
    val strand : Char = p._6
    val values : Array[GValue] = p._7 ++ p._11
    Some((p._1, p._3, start, stop, strand, values))
  }

  def joinRegionsRightMinDistance(p : (Long, Long, String, Long, Long, Char, Array[GValue], Long, Long, Char, Array[GValue], Long, Int)) : Option[FlinkRegionType] = {
    val start : Long = p._8
    val stop : Long = p._9
    val strand : Char = p._10
    val values : Array[GValue] = p._7 ++ p._11
    Some((p._1, p._3, start, stop, strand, values))
  }

  def joinRegionsIntersectionMinDistance(p : (Long, Long, String, Long, Long, Char, Array[GValue], Long, Long, Char, Array[GValue], Long, Int)) : Option[FlinkRegionType] = {
    if(p._4 < p._9 && p._5 > p._8) {
      val start: Long = Math.max(p._4, p._8)
      val stop : Long = Math.min(p._5, p._9)
      val strand: Char = if (p._6.equals(p._10)) p._6 else '*' //TODO
      val values: Array[GValue] = p._7 ++ p._11
      Some((p._1, p._3, start, stop, strand, values))
    } else {
      None
    }
  }

  def joinRegionsContigMinDistance(p : (Long, Long, String, Long, Long, Char, Array[GValue], Long, Long, Char, Array[GValue], Long, Int)) : Option[FlinkRegionType] = {
    val start : Long = Math.min(p._4, p._8)
    val stop : Long = Math.max(p._5, p._9)
    val strand : Char = if(p._6.equals(p._10)) p._6 else '*' // TODO
    val values : Array[GValue] = p._7 ++ p._11
    Some((p._1, p._3, start, stop, strand, values))
  }


  def joinRegions(p : (Long, String, Long, Long, Char, Array[GValue], Long, Long, Char, Array[GValue], Long), regionBuilder : RegionBuilder) : Option[FlinkRegionType] = {
    regionBuilder match {
      case RegionBuilder.LEFT => joinRegionsLeft(p)
      case RegionBuilder.RIGHT => joinRegionsRight(p)
      case RegionBuilder.INTERSECTION => joinRegionsIntersection(p)
      case RegionBuilder.CONTIG => joinRegionsContig(p)
    }
  }

  def joinRegionsLeft(p : (Long, String, Long, Long, Char, Array[GValue], Long, Long, Char, Array[GValue], Long)) : Option[FlinkRegionType] = {
    val start : Long = p._3
    val stop : Long = p._4
    val strand : Char = p._5
    val values : Array[GValue] = p._6 ++ p._10
    Some((p._1, p._2, start, stop, strand, values))
  }

  def joinRegionsRight(p : (Long, String, Long, Long, Char, Array[GValue], Long, Long, Char, Array[GValue], Long)) : Option[FlinkRegionType] = {
    val start : Long = p._7
    val stop : Long = p._8
    val strand : Char = p._9
    val values : Array[GValue] = p._6 ++ p._10
    Some((p._1, p._2, start, stop, strand, values))
  }

  def joinRegionsIntersection(p : (Long, String, Long, Long, Char, Array[GValue], Long, Long, Char, Array[GValue], Long)) : Option[FlinkRegionType] = {
    if(p._3 < p._8 && p._4 > p._7) {
      val start: Long = Math.max(p._3, p._7)
      val stop : Long = Math.min(p._4, p._8)
      val strand: Char = if (p._5.equals(p._9)) p._5 else '*' //TODO
      val values: Array[GValue] = p._6 ++ p._10
      Some((p._1, p._2, start, stop, strand, values))
    } else {
      None
    }
  }

  def joinRegionsContig(p : (Long, String, Long, Long, Char, Array[GValue], Long, Long, Char, Array[GValue], Long)) : Option[FlinkRegionType] = {
    val start : Long = Math.min(p._3, p._7)
    val stop : Long = Math.max(p._4, p._8)
    val strand : Char = if(p._5.equals(p._9)) p._5 else '*' // TODO
    val values : Array[GValue] = p._6 ++ p._10
    Some((p._1, p._2, start, stop, strand, values))
  }


  ////////////////////////////////////////////////////
  //others
  ////////////////////////////////////////////////////

  // utilities

  def distanceCalculator(a : (Long, Long), b : (Long, Long)) : Long = {
    // b to right of a
    if(b._1 >= a._2){
      b._1 - a._2
    } else {
      // b to left of a
      if(b._2 <= a._1){
        a._1 - b._2
      } else {
        // intersecting
        Math.max(a._1, b._1) - Math.min(a._2, b._2)
      }
    }
  }

  def isUpstream(b1:Long, e1:Long, c1:Char, b2:Long, e2:Long) = {
    if ((c1 == '*' || c1 == '+')){
      if (e1 <= b2) true else false
    } else {
      if (b1 >= e2) true else false
    }
  }
  def isDownstream(b1:Long, e1:Long, c1:Char, b2:Long, e2:Long)= {
    if ((c1 == '*' || c1 == '+')){
      if (b1 >= e2) true else false
    } else {
      if (e1 <= b2) true else false
    }
  }
  def build_condition(mindist : Option[Long], maxdist : Long, stream : Option[Char])
      : ((Long, Long, Char, Long, Long) => Option[Long]) = {
    val stream_check = stream match {
      case Some('+') => (b1:Long, e1:Long,c1:Char,b2:Long,e2:Long) => isUpstream(b1,e1,c1,b2,e2)
      case Some('-') => (b1:Long, e1:Long,c1:Char,b2:Long,e2:Long) => isDownstream(b1,e1,c1,b2,e2)
      case _ => (b1:Long, e1:Long,c1:Char,b2:Long,e2:Long) => true
    }

    val min_dist_check =
      if (mindist.isDefined) (x:Long) => x > mindist.get
      else (x:Long) => true

    (b1:Long, e1:Long, c1:Char, b2:Long, e2:Long) => {
      val r1 = (b1,e1)
      val r2 = (b2,e2)
      val reg_distance = distanceCalculator(r1,r2)

      if (stream_check(b1,e1,c1,b2,e2) && min_dist_check(reg_distance) && (reg_distance < maxdist))
        Some(reg_distance)
      else
        None
    }
  }

  case class FirstRoundCondition (f : ((Long, Long, Char, Long, Long) => Option[Long])) extends Serializable {}
  class FirstRoundBinCondition (binSize : Long, upstream_shift : Long, downstream_shift : Long) extends Serializable {
    def f(ab : Long, as : Char, eb : Long, bin : Int) = {
      if ((eb / binSize) == bin)
        true
      else {
        val left_shift =  if (as == '*' || as == '+') upstream_shift else downstream_shift
        if (((ab - left_shift)/ binSize) == bin)
          true
        else
          false
      }
    }
  }



  def createExecutionParameters(list : List[AtomicCondition]) : JoinExecutionParameter = {
    def helper(list : List[AtomicCondition], temp : JoinExecutionParameter) : JoinExecutionParameter = {
      if(list.isEmpty){
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
