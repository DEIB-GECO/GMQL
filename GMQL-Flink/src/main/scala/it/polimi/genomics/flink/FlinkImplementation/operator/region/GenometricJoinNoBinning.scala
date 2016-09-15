package it.polimi.genomics.flink.FlinkImplementation.operator.region

import it.polimi.genomics.core.DataStructures.JoinParametersRD.RegionBuilder.RegionBuilder
import it.polimi.genomics.core.DataStructures.JoinParametersRD._
import it.polimi.genomics.core.DataStructures.{OptionalMetaJoinOperator, SomeMetaJoinOperator, MetaJoinOperator, RegionOperator}
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

/**
 * Created by michelebertoni on 21/06/15.
 */
object GenometricJoinNoBinning {
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
              if (!a._1.equals(b._1)) {
                out.collect(HashSet[(Long, Long)]((a._1, b._1)))
              }
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



      ////////////////////////////////////////////////////
      //first round
      ////////////////////////////////////////////////////


      //chr, bin
      val firstRound : DataSet[(Long, Long, String, Long, Long, Char, Array[GValue], Long, Long, Char, Array[GValue], Long)] =
        ds.join(exp).where(1).equalTo(1){
          (r, e, out : Collector[(Long, Long, String, Long, Long, Char, Array[GValue], Long, Long, Char, Array[GValue], Long)]) => {
            val distance : Long = distanceCalculator((r._3, r._4), (e._3, e._4))
            //if(e._9(0).asInstanceOf[GDouble].v.toInt.equals(5)) println((r._3, r._4, e._6, e._7, r._6(0).asInstanceOf[GDouble].v.toInt, e._9(0).asInstanceOf[GDouble].v.toInt, distance))
            if(
              // same strand or one is neutral
                (r._5.equals('*') || e._5.equals('*') || r._5.equals(e._5))
                  &&
              // distance
                (!firstRoundParameters.max.isDefined || firstRoundParameters.max.get >= distance) && (!firstRoundParameters.min.isDefined || firstRoundParameters.min.get < distance)
                  &&
              // upstream downstream
                ( /* NO STREAM  */
                  ( !firstRoundParameters.stream.isDefined ) // nostream
                  ||
                  /*  UPSTREAM  */
                  (
                    firstRoundParameters.stream.get.equals('+') // upstream
                    &&
                    (
                      ((r._5.equals('+') || r._5.equals('*')) && e._4 <= r._3) // reference with positive strand =>  experiment must be earlier
                      ||
                      ((r._5.equals('-')) && e._3 >= r._4) // reference with negative strand => experiment must be later
                    )
                  )
                  ||
                  /* DOWNSTREAM */
                  (
                    firstRoundParameters.stream.get.equals('-') // downstream
                    &&
                    (
                      ((r._5.equals('+') || r._5.equals('*')) && e._3 >= r._4) // reference with positive strand =>  experiment must be later
                      ||
                      ((r._5.equals('-')) && e._4 <= r._3) // reference with negative strand => experiment must be earlier
                    )
                  )
                )
                  &&
              // valid group
                ( !groups.isDefined || groups.get.contains((r._1, e._1)) )
              ){
                val s = new StringBuilder

                s.append(r._1.toString)
                s.append(e._4.toString)

                /*
                val hashId: Long =
                  Hashing.md5().hashString(s.toString, Charsets.UTF_8).asLong()
                */

                val hashId : Long = Hashing.md5().newHasher().putLong(r._1).putLong(e._4).hash().asLong()


                s.setLength(0)

                s.append(r._1.toString)
                s.append(r._2.toString)
                s.append(r._3.toString)
                s.append(r._4.toString)
                s.append(r._5.toString)
                s.append(r._6.mkString("§"))

                val aggregationId: Long =
                  Hashing.md5().hashString(s.toString, Charsets.UTF_8).asLong()

                out.collect(hashId, aggregationId, r._2, r._3, r._4, r._5, r._6, e._3, e._4, e._5, e._6, distance)
            }
        }
      }


      ////////////////////////////////////////////////////
      //minDistance if needed
      ////////////////////////////////////////////////////


      // (  0     ,   1  ,   2,   3   ,   4  ,   5    ,   6    ,   7   ,   8  ,   9    ,   10   ,   11     )
      // (._1     , ._2  , ._3, ._4   , ._5  , ._6    , ._7    , ._8   , ._9  , ._10   , ._11   , ._12     )
      // (sampleId, AggId, chr, rStart, rStop, rStrand, rValues, eStart, eStop, eStrand, eValues, distance )
      val minDistance : DataSet[(Long, Long, String, Long, Long, Char, Array[GValue], Long, Long, Char, Array[GValue], Long)] =
        if (minDistanceParameter.isDefined) {
          firstRound
            .groupBy(0,1)
            .sortGroup(11 , Order.ASCENDING)
            .reduceGroup((i : Iterator[(Long, Long, String, Long, Long, Char, Array[GValue], Long, Long, Char, Array[GValue], Long)], out : Collector[(Long, Long, String, Long, Long, Char, Array[GValue], Long, Long, Char, Array[GValue], Long)]) => {

              minDistanceSelector(i, minDistanceParameter.get.asInstanceOf[MinDistance].number, Long.MinValue, out)
            /*
              var count = 0
              while(i.hasNext && count < minDistanceParameter.get.asInstanceOf[MinDistance].number){
                out.collect(i.next())
                count += 1;
              }
            */

            })
        } else {
          firstRound
        }


      ////////////////////////////////////////////////////
      //second round if needed
      ////////////////////////////////////////////////////

      // (  0  ,   1  ,   2,   3   ,   4  ,   5    ,   6    ,   7   ,   8  ,   9    ,   10   ,   11    )
      // (._1  , ._2  , ._3, ._4   , ._5  , ._6    , ._7    , ._8   , ._9  , ._10   , ._11   , ._12    )
      // (newID, AggId, chr, rStart, rStop, rStrand, rValues, eStart, eStop, eStrand, eValues, distance)
      val res: DataSet[FlinkRegionType] =
        if (secondRoundParameters.max.isDefined || secondRoundParameters.min.isDefined || secondRoundParameters.stream.isDefined) {
          minDistance.flatMap((p, out: Collector[FlinkRegionType]) => {
            val distance = p._12
            if (
              // same strand or one is neutral
                (p._6.equals('*') || p._10.equals('*') || p._6.equals(p._10)) &&
                // distance
                (!secondRoundParameters.max.isDefined || secondRoundParameters.max.get >= distance) && (!secondRoundParameters.min.isDefined || secondRoundParameters.min.get < distance) &&
                // upstream downstream
                ( /* NO STREAM  */
                  ( !secondRoundParameters.stream.isDefined ) // nostream
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
              val tuple = joinRegions(p, regionBuilder)
              if (tuple.isDefined){
                out.collect(tuple.get)
              }
            }
          })
        } else {
          minDistance.flatMap((p, out : Collector[FlinkRegionType]) => {
            val tuple = joinRegions(p, regionBuilder)
            if (tuple.isDefined){
              out.collect(tuple.get)
            }
          })
        }
      res
    })
    ////////////////////////////////////////////////////
    // Union of all join quadruple results
    ////////////////////////////////////////////////////
    .reduce((a : DataSet[FlinkRegionType], b : DataSet[FlinkRegionType]) => {
      a.union(b)
    })
  }



  ////////////////////////////////////////////////////
  //mindistance
  ////////////////////////////////////////////////////

  @tailrec
  def minDistanceSelector(i : Iterator[(Long, Long, String, Long, Long, Char, Array[GValue], Long, Long, Char, Array[GValue], Long)], currentCount : Int, currentValue : Long, out : Collector[(Long, Long, String, Long, Long, Char, Array[GValue], Long, Long, Char, Array[GValue], Long)]) : Unit = {
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


  // builders

  // (._1  , ._2  , ._3, ._4   , ._5  , ._6    , ._7    , ._8   , ._9  , ._10   , ._11   , ._12    )
  // (newID, AggId, chr, rStart, rStop, rStrand, rValues, eStart, eStop, eStrand, eValues, distance)
  def joinRegions(p : (Long, Long, String, Long, Long, Char, Array[GValue], Long, Long, Char, Array[GValue], Long), regionBuilder : RegionBuilder) : Option[FlinkRegionType] = {
    regionBuilder match {
      case RegionBuilder.LEFT => joinRegionsLeft(p)
      case RegionBuilder.RIGHT => joinRegionsRight(p)
      case RegionBuilder.INTERSECTION => joinRegionsIntersection(p)
      case RegionBuilder.CONTIG => joinRegionsContig(p)
    }
  }

  def joinRegionsLeft(p : (Long, Long, String, Long, Long, Char, Array[GValue], Long, Long, Char, Array[GValue], Long)) : Option[FlinkRegionType] = {
    val start : Long = p._4
    val stop : Long = p._5
    val strand : Char = p._6
    val values : Array[GValue] = p._7 ++ p._11
    Some((p._1, p._3, start, stop, strand, values))
  }

  def joinRegionsRight(p : (Long, Long, String, Long, Long, Char, Array[GValue], Long, Long, Char, Array[GValue], Long)) : Option[FlinkRegionType] = {
    val start : Long = p._8
    val stop : Long = p._9
    val strand : Char = p._10
    val values : Array[GValue] = p._7 ++ p._11
    Some((p._1, p._3, start, stop, strand, values))
  }

  def joinRegionsIntersection(p : (Long, Long, String, Long, Long, Char, Array[GValue], Long, Long, Char, Array[GValue], Long)) : Option[FlinkRegionType] = {
    if(p._4 < p._9 && p._5 > p._8) {
        val start: Long = Math.max(p._4, p._8)
        val stop : Long = Math.min(p._5, p._9)
        val strand: Char = if (p._6.equals(p._10)) p._6 else '*'
        val values: Array[GValue] = p._7 ++ p._11
        Some((p._1, p._3, start, stop, strand, values))
    } else {
      None
    }
  }

  def joinRegionsContig(p : (Long, Long, String, Long, Long, Char, Array[GValue], Long, Long, Char, Array[GValue], Long)) : Option[FlinkRegionType] = {
    val start : Long = Math.min(p._4, p._8)
    val stop : Long = Math.max(p._5, p._9)
    val strand : Char = if(p._6.equals(p._10)) p._6 else '*'
    val values : Array[GValue] = p._7 ++ p._11
    Some((p._1, p._3, start, stop, strand, values))
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
