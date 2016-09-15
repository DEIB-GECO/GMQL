package it.polimi.genomics.flink.FlinkImplementation.operator.region

import it.polimi.genomics.core.DataStructures.JoinParametersRD.RegionBuilder.RegionBuilder
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

/**
 * Created by michelebertoni on 21/06/15.
 */
object GenometricJoin3 {
  final val logger = LoggerFactory.getLogger(this.getClass)

  //private final val BINNING_PARAMETER = 5000
  //private final val MAXIMUM_DISTANCE = 50 * BINNING_PARAMETER

  @throws[SelectFormatException]
  def apply(executor : FlinkImplementation, metajoinCondition : OptionalMetaJoinOperator, joinCondition : List[JoinQuadruple], regionBuilder : RegionBuilder, leftDataset : RegionOperator, rightDataset : RegionOperator, env : ExecutionEnvironment, binSize : Long, maximumDistance : Long) : DataSet[FlinkRegionType] = {
    // load datasets

    val ds : DataSet[FlinkRegionType] =
      executor.implement_rd(leftDataset, env)
    val exp : DataSet[FlinkRegionType] =
      executor.implement_rd(rightDataset, env)

    // load grouping
    /*
    val groups: DataSet[(Long, Long)] =
      if(metajoinCondition.isDefined){
        executor.implement_mjd3(metajoinCondition.get, env)
      } else {
        ds.map(_._1).distinct((t) => t).map((t) => (t,1L))
          .union(
            exp.map(_._1).distinct((t) => t).map((t) => (t,1L))
          )
      }
      */

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

    val binnedExp : DataSet[(Int, Int, Int, Long, String, Long, Long, Char, Array[GValue])] =
      binExperiment(exp, binSize)
    // (BinStart, binStop, bin, SampleId, chr, start, stop, strand, values)

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
      //bin anchor
      ////////////////////////////////////////////////////

      // extend reference to join condition
      // bin reference
      val binnedAnchor : DataSet[(Long, String, Long, Long, Char, Array[GValue], Long, Int, Int)] =
        binAnchor(ds, firstRoundParameters, secondRoundParameters, binSize, maximumDistance)
      //(  0        ,   1       ,   2      ,   3    ,   4    ,   5    ,   6      ,   7           ,   8          ,    9     )
      //(._1        , ._2       , ._3      , ._4    , ._5    , ._6    , ._7      , ._8           , ._9          , ._10     )
      //(sampleId   , chr       , start    , stop   , strand , values , aggId    , binStart      , bin          )
      //(binStart   , binStop   , bin      , id     , chr    , start  , stop     , strand        , values       )



      ////////////////////////////////////////////////////
      //first round
      ////////////////////////////////////////////////////


      //binnedAnchor.map((v) => (v._1, v._2,v ._3, v._4, v._5, v._6(0), v._7, v._8, v._9)).print
      //binnedExp.map((v) => (v._1, v._2, v._3, v._4, v._5, v._6, v._7, v._8, v._9(0))).print

      //chr, bin
      val firstRound =
        binnedAnchor.join(binnedExp).where(1,8).equalTo(4,2){
          (r, e, out : Collector[(Long, Long, String, Long, Long, Char, Array[GValue], Long, Long, Char, Array[GValue], Long, Int)]) => {
            val distance : Long = distanceCalculator((r._3, r._4), (e._6, e._7))
            //if(e._9(0).asInstanceOf[GDouble].v.toInt.equals(5)) println((r._3, r._4, e._6, e._7, r._6(0).asInstanceOf[GDouble].v.toInt, e._9(0).asInstanceOf[GDouble].v.toInt, distance))
            if(
            // first match
            //((e._1.equals(e._3)) || (r._8.equals(r._9)))
              (
                ((e._6 < r._3 && e._7 <= r._4) && (e._2.equals(e._3))) //left and lastExpBin = current
                  ||
                  ((r._3 <= e._6 && r._4 < e._7) && (e._1.equals(e._3))) //right and firstExpBin = current
                  ||
                  ((r._3 <= e._6 && e._6 <= r._4 && r._3 <= e._7 && e._7 <= r._4) && (e._1.equals(e._3))) //included and firstBinExp = current
                  ||
                  ((e._6 < r._3 && r._3 < e._7 && e._6 < r._4 && r._4 < e._7) && (r._8.equals(r._9))) //including and firstBinRef = current
                )
                //
                &&
                // same strand or one is neutral
                (r._5.equals('*') || e._8.equals('*') || r._5.equals(e._8))
                &&
                // distance
                (firstRoundParameters.max.isEmpty || firstRoundParameters.max.get > distance)
                &&
                (firstRoundParameters.min.isEmpty || firstRoundParameters.min.get < distance)
                &&
                // upstream downstream
                ( /* NO STREAM  */
                  ( firstRoundParameters.stream.isEmpty ) // nostream
                    ||
                    /*  UPSTREAM  */
                    (
                      firstRoundParameters.stream.get.equals('+') // upstream
                        &&
                        (
                          ((r._5.equals('+') || r._5.equals('*')) && e._7 <= r._3) // reference with positive strand =>  experiment must be earlier
                            ||
                            ((r._5.equals('-')) && e._6 >= r._4) // reference with negative strand => experiment must be later
                          )
                      )
                    ||
                    /* DOWNSTREAM */
                    (
                      firstRoundParameters.stream.get.equals('-') // downstream
                        &&
                        (
                          ((r._5.equals('+') || r._5.equals('*')) && e._6 >= r._4) // reference with positive strand =>  experiment must be later
                            ||
                            ((r._5.equals('-')) && e._7 <= r._3) // reference with negative strand => experiment must be earlier
                          )
                      )
                  )
                &&
                // valid group
                ( groups.isEmpty || groups.get.contains((r._1, e._4)) )
            ){
              val s = new StringBuilder

              s.append(r._1.toString)
              s.append(e._4.toString)

              /*
              val hashId: Long =
                Hashing.md5().hashString(s.toString, Charsets.UTF_8).asLong()
              */
              val hashId : Long = Hashing.md5().newHasher().putLong(r._1).putLong(e._4).hash().asLong()

              out.collect(hashId, r._7, r._2, r._3, r._4, r._5, r._6, e._6, e._7, e._8, e._9, distance, r._9)
            }
          }
        }
          .withForwardedFieldsFirst("6->1;1->2;2->3;3->4;4->5;8->12")
          .withForwardedFieldsSecond("5->7;6->8;7->9")
      ////////////////////////////////////////////////////
      //if bin split happened than a distinction is necessary
      //example is an anchor splitted with an exp that is crossing both the split
      ////////////////////////////////////////////////////



      //firstRound.map((v) => (v._1, v._7(0).asInstanceOf[GDouble].v.toInt, v._11(0).asInstanceOf[GDouble].v.toInt, v._12)).print
      /*
            val cleanedFirstRound =
              if(firstRoundParameters.min.isDefined){
                firstRound.distinct
              } else {
                firstRound
              }

      */

      ////////////////////////////////////////////////////
      //minDistance if needed
      ////////////////////////////////////////////////////


      // (  0     ,   1  ,   2,   3   ,   4  ,   5    ,   6    ,   7   ,   8  ,   9    ,   10   ,   11    ,   12 )
      // (._1     , ._2  , ._3, ._4   , ._5  , ._6    , ._7    , ._8   , ._9  , ._10   , ._11   , ._12    , ._13 )
      // (sampleId, AggId, chr, rStart, rStop, rStrand, rValues, eStart, eStop, eStrand, eValues, distance, bin  )
      val minDistance =
        if (minDistanceParameter.isDefined) {
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
              /*
                var count = 0
                while(i.hasNext && count < minDistanceParameter.get.asInstanceOf[MinDistance].number){
                  out.collect(i.next())
                  count += 1;
                }
              */
            }).withForwardedFields("0;1;2;3;4;5;7;8;9;11;12")
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
  //anchor
  ////////////////////////////////////////////////////

  /*

  def assignAnchorGroups(executor : FlinkImplementation, ds: DataSet[FlinkRegionType], groups: DataSet[(Long, Long)], env : ExecutionEnvironment): DataSet[(Long, Long, String, Long, Long, Char, Array[GValue], Long)] = {
    ds.join(groups).where(0).equalTo(0) {
      (region : FlinkRegionType, group : (Long, Long), out : Collector[(Long, Long, String, Long, Long, Char, Array[GValue], Long)]) => {

        val s = new StringBuilder

        s.append(region._1.toString)
        s.append(group._2.toString)

        val hashId: Long =
          Hashing.md5().hashString(s.toString, Charsets.UTF_8).asLong()

        s.setLength(0)

        s.append(region._1.toString)
        s.append(group._2.toString)
        s.append(region._2.toString)
        s.append(region._3.toString)
        s.append(region._4.toString)
        s.append(region._5.toString)
        s.append(region._6.mkString("§"))

        val aggregationId: Long =
          Hashing.md5().hashString(s.toString, Charsets.UTF_8).asLong()

        out.collect((hashId, group._2, region._2, region._3, region._4, region._5, region._6, aggregationId))

      }
    }
  }

  */

  def binAnchor(ds : DataSet[(Long, String, Long, Long, Char, Array[GValue])], firstRound : JoinExecutionParameter, secondRound : JoinExecutionParameter, binSize : Long, max : Long) : DataSet[(Long, String, Long, Long, Char, Array[GValue], Long, Int, Int)] = {
    //(  0        ,   1    ,   2      ,   3  ,   4   ,   5   ,   6   ,   7          )
    //(._1        , ._2    , ._3      , ._4  , ._5   , ._6   , ._7   , ._8          )
    //(sampleId   , chr    , start    , stop , strand, values )
    ds.flatMap((r : (Long, String, Long, Long, Char, Array[GValue]), out : Collector[(Long, String, Long, Long, Char, Array[GValue], Long, Int, Int)]) => {


      val aggregationId: Long =
        Hashing.md5().newHasher()
          .putLong(r._1)
          .putString(r._2, Charsets.UTF_8)
          .putLong(r._3)
          .putLong(r._4)
          .putChar(r._5)
          .putString(r._6.mkString("§"), Charsets.UTF_8)
          .hash().asLong()


      val maxDistance : Long =
        if(firstRound.max.isDefined) firstRound.max.get
        else if(secondRound.max.isDefined) Math.max(secondRound.max.get, max)
        else max
      val start1 : Long = if(firstRound.stream.isEmpty || (firstRound.stream.get.equals(r._5)) || (r._5.equals('*') && firstRound.stream.get.equals('+')) ) r._3 - maxDistance else r._4
      val end1 : Long = if(firstRound.min.isDefined) r._3 - firstRound.min.get else 0L
      val split : Boolean = firstRound.min.isDefined
      val start2 : Long = if(firstRound.min.isDefined) r._4 + firstRound.min.get else 0L
      val end2 : Long = if(firstRound.stream.isEmpty || (!firstRound.stream.get.equals(r._5)) || (r._5.equals('*') && firstRound.stream.get.equals('-')) ) r._4 + maxDistance else  r._3

      if(split){

        //(binStart, bin)
        val binPairs : Set[(Int, Int)] =
          computeBins(start1, end1, start2, end2, binSize, r._3)

        for(p <- binPairs){
          out.collect((r._1, r._2, r._3, r._4, r._5, r._6, aggregationId, p._1, p._2))
        }

      } else {

        val keyBin = (r._3 / binSize).toInt
        val binStart = if ( (start1 / binSize).toInt < 0 ) 0 else (start1 / binSize).toInt
        val binEnd = (end2 / binSize).toInt
        for (i <- binStart to binEnd) {
          out.collect((r._1, r._2, r._3, r._4, r._5, r._6, aggregationId, keyBin, i))
        }

      }
    })
  }
  //(sampleId, groupId, chr, start, stop, strand, values, aggregationId, originalStart, originalStop)
  //(SampleId, groupId, chr, start, stop, strand, values, aggregationId, originalStart, originalStop, binStart, bin)


  def computeBins(start1 : Long, end1 : Long, start2 : Long, end2 : Long, binSize : Long, regionStart : Long) : Set[(Int, Int)] ={

    val keyBin = (regionStart / binSize).toInt

    val a : Seq[(Int, Int)] =
    //if(!stream.isDefined || regionStream.equals(stream.get)){
      if(end1 > start1){
        val binStart1 = if ( (start1 / binSize).toInt < 0 ) 0 else (start1 / binSize).toInt
        val binEnd1 = if ( (end1 / binSize).toInt < 0 ) 0 else (end1 / binSize).toInt
        (binStart1 to binEnd1).map((v) => (keyBin, v))
        /*
          for (i <- binStart1 to binEnd1) {
            out.collect((binStart1, i))
          }
        */
      } else {
        List()
      }

    val b : Seq[(Int, Int)] =
    //if(!stream.isDefined || !regionStream.equals(stream.get)){
      if(end2 > start2){
        val binStart2 = (start2 / binSize).toInt
        val binEnd2 = (end2 / binSize).toInt
        (binStart2 to binEnd2).map((v) => (keyBin, v))
        /*
          for (i <- binStart2 to binEnd2) {
            out.collect((binStart2, i))
          }
        */
      } else {
        List()
      }

    (a ++ b).toSet
  }


  ////////////////////////////////////////////////////
  //experiment (sample)
  ////////////////////////////////////////////////////

  /*
  def binExperiment(ds: DataSet[FlinkRegionType]): DataSet[(Int, Int, Long, String, Long, Long, Char, Array[GValue])] = {
    ds.flatMap((e, out:Collector[(Int, Int, Long, String, Long, Long, Char, Array[GValue])]) => {
      val binStart = (e._3 / BINNING_PARAMETER).toInt
      val binEnd = (e._4 / BINNING_PARAMETER).toInt
      for (i <- binStart to binEnd) {
        out.collect((binStart, i, e._1, e._2, e._3, e._4, e._5, e._6))
      }
    })
  }
  */

  def binExperiment(ds: DataSet[FlinkRegionType], binSize : Long): DataSet[(Int, Int, Int, Long, String, Long, Long, Char, Array[GValue])] = {
    //assignExperimentGroups(executor : FlinkImplementation, ds: DataSet[FlinkRegionType], groups: DataSet[(Long, Long)], env : ExecutionEnvironment): DataSet[(Long, Int, Int, Long, String, Long, Long, Char, Array[GValue])] = {
    //ds.join(groups).where(0).equalTo(0) {
    //(region : FlinkRegionType, group : (Long, Long), out : Collector[(Long, Int, Int, Long, String, Long, Long, Char, Array[GValue], Long)]) => {
    ds.flatMap((region : FlinkRegionType, out : Collector[(Int, Int, Int, Long, String, Long, Long, Char, Array[GValue])]) => {
      val binStart = (region._3 / binSize).toInt
      val binEnd = (region._4 / binSize).toInt
      for (i <- binStart to binEnd) {
        out.collect((binStart, binEnd, i, region._1, region._2, region._3, region._4, region._5, region._6))
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


  // builders

  // (._1  , ._2  , ._3, ._4   , ._5  , ._6    , ._7    , ._8   , ._9  , ._10   , ._11   , ._12    )
  // (newID, AggId, chr, rStart, rStop, rStrand, rValues, eStart, eStop, eStrand, eValues, distance)
  def joinRegions(p : (Long, Long, String, Long, Long, Char, Array[GValue], Long, Long, Char, Array[GValue], Long, Int), regionBuilder : RegionBuilder) : Option[FlinkRegionType] = {
    regionBuilder match {
      case RegionBuilder.LEFT => joinRegionsLeft(p)
      case RegionBuilder.RIGHT => joinRegionsRight(p)
      case RegionBuilder.INTERSECTION => joinRegionsIntersection(p)
      case RegionBuilder.CONTIG => joinRegionsContig(p)
    }
  }

  def joinRegionsLeft(p : (Long, Long, String, Long, Long, Char, Array[GValue], Long, Long, Char, Array[GValue], Long, Int)) : Option[FlinkRegionType] = {
    val start : Long = p._4
    val stop : Long = p._5
    val strand : Char = p._6
    val values : Array[GValue] = p._7 ++ p._11
    Some((p._1, p._3, start, stop, strand, values))
  }

  def joinRegionsRight(p : (Long, Long, String, Long, Long, Char, Array[GValue], Long, Long, Char, Array[GValue], Long, Int)) : Option[FlinkRegionType] = {
    val start : Long = p._8
    val stop : Long = p._9
    val strand : Char = p._10
    val values : Array[GValue] = p._7 ++ p._11
    Some((p._1, p._3, start, stop, strand, values))
  }

  def joinRegionsIntersection(p : (Long, Long, String, Long, Long, Char, Array[GValue], Long, Long, Char, Array[GValue], Long, Int)) : Option[FlinkRegionType] = {
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

  def joinRegionsContig(p : (Long, Long, String, Long, Long, Char, Array[GValue], Long, Long, Char, Array[GValue], Long, Int)) : Option[FlinkRegionType] = {
    val start : Long = Math.min(p._4, p._8)
    val stop : Long = Math.max(p._5, p._9)
    val strand : Char = if(p._6.equals(p._10)) p._6 else '*' // TODO
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
