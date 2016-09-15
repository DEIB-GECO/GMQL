package it.polimi.genomics.spark.implementation.RegionsOperators.GenometricMap

import com.google.common.hash.Hashing
import it.polimi.genomics.core.DataStructures._
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.{GValue, GDouble, GString}
import it.polimi.genomics.core.GRecordKey
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory


/**
 * Created by abdulrahman kaitoua on 13/07/15.
 */
object GenometricMap2 {


  private final val logger = LoggerFactory.getLogger(this.getClass);
  @throws[SelectFormatException]
  def apply(executor : GMQLSparkExecutor, grouping : OptionalMetaJoinOperator, aggregator : List[RegionAggregate.RegionsToRegion], reference : RegionOperator, experiments : RegionOperator, BINNING_PARAMETER:Long,sc : SparkContext) : RDD[GRECORD] = {

    //creating the datasets
    val ref: RDD[GRECORD] = executor.implement_rd(reference, sc)
    val exp: RDD[GRECORD] = executor.implement_rd(experiments, sc)

    execute(executor, grouping, aggregator, ref, exp,BINNING_PARAMETER, sc)
  }

  @throws[SelectFormatException]
  def execute(executor : GMQLSparkExecutor, grouping : OptionalMetaJoinOperator, aggregator : List[RegionAggregate.RegionsToRegion], ref: RDD[GRECORD], exp: RDD[GRECORD], BINNING_PARAMETER:Long, sc : SparkContext) : RDD[GRECORD] = {
    //creating the groups if they exist
    //otherwise create all possible (ref,exp) pairs
    val pairs: RDD[SparkMetaJoinType] =
//      if(grouping.isInstanceOf[SomeMetaJoinOperator]){
          executor.implement_mjd(grouping, sc)
//      } else {
//        ref.map(_._1._1).distinct.cartesian(exp.map(_._1._1).distinct()).groupByKey().mapValues(x=>x.toArray)
//      }
    val Bgroups = sc.broadcast(pairs.collectAsMap())
    //(sampleID, sampleID)

    //group the datasets
    //(expID, Chr, Bin)(refID, Stat, Stop, Strand, Values, Agg, BinStart)
    val groupedRef: RDD[((Long, String, Int), (Long, Long, Long, Char, Array[GValue], Long, Int))] =
      assignRegionGroups(ref,Bgroups,BINNING_PARAMETER).distinct
    //(expID, Chr, Bin)(Start, Stop, Strand, Values, BinStart)
    val groupedExp: RDD[((Long, String, Int), (Long, Long, Char, Array[GValue], Int))] =
      assignExperimentGroups(exp,BINNING_PARAMETER)

    //prepare a neutral extension of the array that matches the aggregator types
    //neutral means GInt(0), GDouble(0,0) and GString("")
    val extraData: Array[List[GValue]] =
      aggregator.map(_ => List[GValue]()).toArray


    //Join phase
    val coGroupResult: RDD[(Long, (Long, String, Long, Long, Char, Array[GValue], Array[List[GValue]], Int))] =
      groupedRef // expID, bin, chromosome
        .cogroup(groupedExp).flatMap{x=> val references = x._2._1; val experiments = x._2._2
          val refCollected : List[PartialResult] = references.map(r => new PartialResult(r, 0, List[Array[List[GValue]]]() )).toList
          for(e <- experiments){
            for(r <- refCollected){
              if(/* cross */
              /* space overlapping */
                (r.binnedRegion._2 < e._2 && e._1 < r.binnedRegion._3)
                  && /* same strand */
                  (r.binnedRegion._4.equals('*') || e._3.equals('*') || r.binnedRegion._4.equals(e._3))
                  && /* first comparison */
                  (r.binnedRegion._7.equals(x._1._3) ||  e._5.equals(x._1._3))
              ) {
                r.count += 1
                r.extra = r.extra :+ e._4.foldLeft(new Array[List[GValue]](0))((z : Array[List[GValue]], v : GValue) => z :+ List(v))
              }
            }
          }
          refCollected.map{pr =>
            val extra = if(!pr.extra.isEmpty)pr.extra.reduce((a,b) => a.zip(b).map((p) => p._1 ++ p._2))else Array[List[GValue]]()
            (pr.binnedRegion._6,
              (Hashing.md5().newHasher().putLong(pr.binnedRegion._1).putLong(x._1._1).hash.asLong,
                x._1._2, pr.binnedRegion._2, pr.binnedRegion._3, pr.binnedRegion._4, pr.binnedRegion._5, extra, pr.count)
              )
          }
        }


    //Aggregation phase
    val aggregationResult=
      coGroupResult
        //reduce phase
        //concatenation of extra data
        .reduceByKey(
          (r1,r2) =>
            (r1._1, r1._2, r1._3, r1._4, r1._5, r1._6,
              r1._7
                .zip(r2._7)
                .map((a) => a._1 ++ a._2),
              r1._8 + r2._8)
        )
        //apply aggregation function on extra data
        .map(l => {
        (new GRecordKey(l._2._1, l._2._2, l._2._3, l._2._4, l._2._5), l._2._6 ++ ( GDouble(l._2._8) +: {
          aggregator
            .map((f : RegionAggregate.RegionsToRegion) => {
            f.fun(
              l._2._7(f.index)
            )
          })
        }))
      })

    //OUTPUT
    aggregationResult
  }

  /**
   * Create an extension array of the type consistent with the aggregator types
   * @param aggregator list of aggregator data
   * @return the extension array
   */
  def createSampleArrayExtension(aggregator : List[RegionAggregate.RegionsToRegion], sample : Array[GValue]) : Array[List[GValue]] = {
    //call the Helper using an array accumulator
    createSampleArrayExtensionHelper(aggregator, sample, List(List())).toArray
  }

  /**
   * Helper to the extension creator
   * recursive function that
   * for each element of the array add one element of the respective type to the accumulator
   * if the set is empty return the accumulator
   * @param aggregator list of aggregator data
   * @param acc current accumulator
   * @return the extension array
   */
  def createSampleArrayExtensionHelper(aggregator : List[RegionAggregate.RegionsToRegion], sample : Array[GValue], acc : List[List[GValue]]) : List[List[GValue]] = {
    if(aggregator.size.equals(0)){
      acc
    } else {
      sample(aggregator(0).index) match{
        //case GInt(_) => createSampleArrayExtensionHelper(aggregator.drop(1), sample, acc :+ GInt(0))
        case GDouble(_) => createSampleArrayExtensionHelper(aggregator.drop(1), sample, acc :+ List(GDouble(0)))
        case GString(_) => createSampleArrayExtensionHelper(aggregator.drop(1), sample, acc :+ List(GString("")))
        case _ => createSampleArrayExtensionHelper(aggregator.drop(1), sample, acc :+ List(GString("")))
      }
    }
  }

  //(expID, Chr, Bin)(refID, Stat, Stop, Strand, Values, Agg, BinStart)
  def assignRegionGroups( ds: RDD[GRECORD], Bgroups:  Broadcast[collection.Map[Long, Array[Long]]],BINNING_PARAMETER:Long): RDD[((Long, String, Int), (Long, Long, Long, Char, Array[GValue], Long, Int))] ={//: RDD[(Long, Long, Int, Int, Long, String, Long, Long, Char, Array[GValue], Long)] = {
    ds.flatMap {region =>
      val group = Bgroups.value
      val expID = group.get(region._1._1)
      if(expID.isDefined)
      {
        expID.get.flatMap{expid =>
          val aggregationId = Hashing.murmur3_128().newHasher().putString(region._1._1+ expid+region._1._2+ region._1._3+ region._1._4+ region._1._5+ region._2.mkString("/"),java.nio.charset.Charset.defaultCharset()).hash.asLong
          val binStart = (region._1._3 / BINNING_PARAMETER).toInt
          val binEnd = (region._1._4 / BINNING_PARAMETER).toInt
          for (i <- binStart to binEnd)
          yield((expid,region._1._2,i),( region._1._1,  region._1._3, region._1._4, region._1._5, region._2, aggregationId,binStart))
        }.toList
      }else {
        logger.warn ("Warn, please check the reference groups. a reference does not have group with any exp");
        List()
      }
    }
  }

  //(expID, Chr, Bin)(Start, Stop, Strand, Values, BinStart)
  def assignExperimentGroups(ds: RDD[GRECORD],BINNING_PARAMETER:Long): RDD[((Long, String, Int), (Long, Long, Char, Array[GValue], Int))] = {
    //ds.joinWithTiny(groups).where(0).equalTo(0){
    ds.flatMap(experiment => {
        val binStart = (experiment._1._3 / BINNING_PARAMETER).toInt
        val binEnd = (experiment._1._4 / BINNING_PARAMETER).toInt
        for (i <- binStart to binEnd)
          yield((experiment._1._1, experiment._1._2, i),(experiment._1._3, experiment._1._4, experiment._1._5, experiment._2,binStart))
      }
    )
  }

  class PartialResult(val binnedRegion : (Long, Long, Long, Char, Array[GValue], Long, Int), var count : Int, var extra : List[Array[List[GValue]]])

}