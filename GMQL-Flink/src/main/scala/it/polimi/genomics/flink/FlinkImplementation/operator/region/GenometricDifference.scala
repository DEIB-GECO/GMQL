package it.polimi.genomics.flink.FlinkImplementation.operator.region

import it.polimi.genomics.core.DataStructures.{OptionalMetaJoinOperator, SomeMetaJoinOperator, MetaJoinOperator, RegionOperator}
import it.polimi.genomics.core.DataTypes.FlinkRegionType
import it.polimi.genomics.core.GValue
import it.polimi.genomics.flink.FlinkImplementation.FlinkImplementation
import org.apache.flink.api.scala._
import org.apache.flink.hadoop.shaded.com.google.common.base.Charsets
import org.apache.flink.hadoop.shaded.com.google.common.hash.Hashing
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

/**
 * Created by michelebertoni on 24/06/15.
 */
object GenometricDifference {
  final val logger = LoggerFactory.getLogger(this.getClass)

  //private final val BINNING_PARAMETER = 50000

  def apply(executor : FlinkImplementation, grouping : OptionalMetaJoinOperator, leftDataset : RegionOperator, rightDataset : RegionOperator, binSize : Long, env : ExecutionEnvironment) : DataSet[FlinkRegionType] = {
    //creating the datasets
    val ref: DataSet[(Long, String, Long, Long, Char, Array[GValue])] =
      executor.implement_rd(leftDataset, env)
    val exp: DataSet[(Long, String, Long, Long, Char, Array[GValue])] =
      executor.implement_rd(rightDataset, env)

    val groups: DataSet[(Long, Long)] =
      if(grouping.isInstanceOf[SomeMetaJoinOperator]){
        executor.implement_mjd3(grouping, env).map((v) => (v._1, v._2))
      } else {
        ref.map(_._1).distinct().map((t) => (t,1L))
          .union(
            exp.map(_._1).distinct().map((t) => (t,1L))
          )
      }

    //group the datasets
    val groupedRef : DataSet[(Long, Int, Int, Long, String, Long, Long, Char, Array[GValue], Long)] =
      assignRegionGroups(executor, ref, groups, binSize, env).distinct(0,1,2,3,4,5,6,7,9)
    val groupedExp : DataSet[(Long, Int, Int, Long, String, Long, Long, Char, Array[GValue])] =
      assignExperimentGroups(executor, exp, groups, binSize, env)

    //Join phase
    val coGroupResult : DataSet[(Long, String, Long, Long, Char, Array[GValue], Int, Long)] =
      groupedRef // groupId, bin, chromosome
        .coGroup(groupedExp).where(0,2,4).equalTo(0,2,4){
        (references : Iterator[(Long, Int, Int, Long, String, Long, Long, Char, Array[GValue], Long)], experiments : Iterator[(Long, Int, Int, Long, String, Long, Long, Char, Array[GValue])], out : Collector[(Long, String, Long, Long, Char, Array[GValue], Int, Long)]) => {
          val refCollected = references.toList.map(x=>PartialResult(x,0))
          for (e <- experiments) {
            for(r <- refCollected) {
              if (
                      /* intersect */
                  (r.r._6 < e._7 && e._6 < r.r._7)
                  &&  /* same strand */
                  (r.r._8.equals('*') || e._8.equals('*') || r.r._8.equals(e._8))
                  // not necessary
                  // && /* first comparison */
                  // (r.r._3.equals(r.r._4) ||  e._1.equals(e._2))
                ){
                r.count = r.count + 1
              }
            }
          }
          for (r<- refCollected) out.collect((r.r._4, r.r._5, r.r._6, r.r._7, r.r._8, r.r._9, r.count, r.r._10))

          /*
          val rCollected = experiments.toSet
          for(region <- references){
            var count = 0
            for(experiment <- rCollected){
              if(/* cross */
                  /* space overlapping */
                  (region._6 < experiment._7 && experiment._6 < region._7)
                  && /* same strand */
                  (region._8.equals('*') || experiment._8.equals('*') || region._8.equals(experiment._8))
                  // not necessary
                  // && /* first comparison */
                  // (region._3.equals(region._4) ||  experiment._1.equals(experiment._2))
              ) {
                count = count + 1;
              }
            }
            out.collect((region._4, region._5, region._6, region._7, region._8, region._9, count, region._10))
          }
          */
        }
      }

    val filteredReduceResult : DataSet[(Long, String, Long, Long, Char, Array[GValue])] =
    // group regions by aggId (same region)
      coGroupResult
        .groupBy(7)
        // reduce phase -> sum the count value of left and right
        .reduce(
          (r1 : (Long, String, Long, Long, Char, Array[GValue], Int, Long) ,r2 : (Long, String, Long, Long, Char, Array[GValue], Int, Long)) =>{
            (r1._1, r1._2, r1._3, r1._4, r1._5, r1._6, r1._7 + r2._7, r1._8)
          }
        )
        // filter only region with count = 0
        .flatMap((r : (Long, String, Long, Long, Char, Array[GValue], Int, Long), out : Collector[FlinkRegionType]) => {
          if(r._7.equals(0)){
            out.collect(r._1, r._2, r._3, r._4, r._5, r._6)
          }
        })

    //OUTPUT
    filteredReduceResult

  }


  def assignRegionGroups(executor : FlinkImplementation, ds: DataSet[FlinkRegionType], groups: DataSet[(Long, Long)], binSize : Long, env : ExecutionEnvironment): DataSet[(Long, Int, Int, Long, String, Long, Long, Char, Array[GValue], Long)] = {
    ds.join(groups).where(0).equalTo(0) {
      (region : FlinkRegionType, group : (Long, Long), out : Collector[(Long, Int, Int, Long, String, Long, Long, Char, Array[GValue], Long)]) => {

        val s = new StringBuilder

        s.append(region._1.toString)
        s.append(group._2.toString)
        s.append(region._2.toString)
        s.append(region._3.toString)
        s.append(region._4.toString)
        s.append(region._5.toString)
        s.append(region._6.mkString("§"))

        val aggregationId: Long =
          Hashing.md5().hashString(s.toString, Charsets.UTF_8).asLong()

        val binStart = (region._3 / binSize).toInt
        val binEnd = (region._4 / binSize).toInt
        for (i <- binStart to binEnd) {
          out.collect((group._2, binStart, i, region._1, region._2, region._3, region._4, region._5, region._6, aggregationId))
        }

      }
    }
  }


  def assignExperimentGroups(executor : FlinkImplementation, ds: DataSet[FlinkRegionType], groups: DataSet[(Long, Long)], binSize : Long, env : ExecutionEnvironment): DataSet[(Long, Int, Int, Long, String, Long, Long, Char, Array[GValue])] = {
    ds.join(groups).where(0).equalTo(0) {
      (region : FlinkRegionType, group : (Long, Long), out : Collector[(Long, Int, Int, Long, String, Long, Long, Char, Array[GValue])]) => {

        val binStart = (region._3 / binSize).toInt
        val binEnd = (region._4 / binSize).toInt
        for (i <- binStart to binEnd) {
          out.collect((group._2, binStart, i, region._1, region._2, region._3, region._4, region._5, region._6))
        }

      }
    }
  }


  sealed case class PartialResult(r : (Long, Int, Int, Long, String, Long, Long, Char, Array[GValue], Long), var count : Int)

}
