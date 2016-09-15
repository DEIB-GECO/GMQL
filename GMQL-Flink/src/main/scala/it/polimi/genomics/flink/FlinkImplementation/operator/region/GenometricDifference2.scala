package it.polimi.genomics.flink.FlinkImplementation.operator.region

import it.polimi.genomics.core.DataStructures.{OptionalMetaJoinOperator, SomeMetaJoinOperator, MetaJoinOperator, RegionOperator}
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.GValue
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.flink.FlinkImplementation.FlinkImplementation
import org.apache.flink.api.scala._
import org.apache.flink.hadoop.shaded.com.google.common.base.Charsets
import org.apache.flink.hadoop.shaded.com.google.common.hash.Hashing
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

/**
 * Created by michelebertoni on 24/06/15.
 */
object GenometricDifference2 {
  final val logger = LoggerFactory.getLogger(this.getClass)

  //private final val BINNING_PARAMETER = 50000

  @throws[SelectFormatException]
  def apply(executor : FlinkImplementation, grouping : OptionalMetaJoinOperator, reference : RegionOperator, experiments : RegionOperator, binSize : Long, env : ExecutionEnvironment) : DataSet[FlinkRegionType] = {

    //creating the datasets
    val ref: DataSet[(Long, String, Long, Long, Char, Array[GValue])] =
      executor.implement_rd(reference, env)
    val exp: DataSet[(Long, String, Long, Long, Char, Array[GValue])] =
      executor.implement_rd(experiments, env)

    execute(executor, grouping, ref, exp, binSize, env)
  }

  @throws[SelectFormatException]
  def execute(executor : FlinkImplementation, grouping : OptionalMetaJoinOperator, ref: DataSet[(Long, String, Long, Long, Char, Array[GValue])], exp: DataSet[(Long, String, Long, Long, Char, Array[GValue])], binSize : Long, env : ExecutionEnvironment) : DataSet[FlinkRegionType] = {
    //creating the groups if they exist
    //otherwise create all possible (ref,exp) pairs
    val pairs: DataSet[(Long, Long)] =
//      if(grouping.isInstanceOf[SomeMetaJoinOperator])
      {
        val groups : DataSet[FlinkMetaJoinType] =
          executor.implement_mjd3(grouping, env)

        //groups.print

        groups
          .join(groups).where(1,2).equalTo(1,3){
          (a, b, out: Collector[(Long, Long)]) => {
            //if (!a._1.equals(b._1)) {
            out.collect((a._1, b._1))
            //}
          }
        }
          .distinct

      }
//      else {
//        ref.map(_._1).distinct((t) => t).cross(exp.map(_._1).distinct((t) => t))
//      }

    //(sampleID, sampleID)

    //group the datasets
    val groupedRef: DataSet[(Long, Long, Int, Int, Long, String, Long, Long, Char, Array[GValue], Long)] =
      assignRegionGroups(executor, ref.distinct(0,1,2,3,4), pairs, binSize, env)
    val groupedExp: DataSet[(Int, Int, Long, String, Long, Long, Char, Array[GValue])] =
      assignExperimentGroups(executor, exp, binSize, env)

    //ref(hashId, expId, binStart, bin, refid, chr, start, stop, strand, GValues, aggregationID)
    //exp(binStart, bin, expId, chr, start, stop., strand, GValues)

    //Join phase
    val coGroupResult : DataSet[(Long, String, Long, Long, Char, Array[GValue], Int, Long)] =
      groupedRef // expID, bin, chromosome
        .coGroup(groupedExp).where(1,3,5).equalTo(2,1,3){
        (references : Iterator[(Long, Long, Int, Int, Long, String, Long, Long, Char, Array[GValue], Long)], experiments : Iterator[(Int, Int, Long, String, Long, Long, Char, Array[GValue])], out : Collector[(Long, String, Long, Long, Char, Array[GValue], Int, Long)]) => {
          val refCollected : List[PartialResult] = references.map((r) => (new PartialResult(r, 0))).toList
          for(e <- experiments){
            for(r <- refCollected){
              if(/* cross */
              /* space overlapping */
                (r.binnedRegion._7 < e._6 && e._5 < r.binnedRegion._8)
                  && /* same strand */
                  (r.binnedRegion._9.equals('*') || e._7.equals('*') || r.binnedRegion._9.equals(e._7))
                  && /* first comparison */
                  (r.binnedRegion._3.equals(r.binnedRegion._4) ||  e._1.equals(e._2))
              ) {
                r.count += 1
              }
            }
          }
          refCollected.map((pr) => {
            //println(pr)
            out.collect(pr.binnedRegion._1, pr.binnedRegion._6, pr.binnedRegion._7, pr.binnedRegion._8, pr.binnedRegion._9, pr.binnedRegion._10, pr.count, pr.binnedRegion._11)
          })
        }
      }

    //Aggregation phase
    val filteredReduceResult : DataSet[(Long, String, Long, Long, Char, Array[GValue])] =
      coGroupResult
        .groupBy(7)
        //reduce phase
        .reduce(
          (r1,r2) =>
            (r1._1, r1._2, r1._3, r1._4, r1._5, r1._6, r1._7 + r2._7, r1._8)
        )
        //apply aggregation function on extra data
        .flatMap((l : (Long, String, Long, Long, Char, Array[GValue], Int, Long), out : Collector[FlinkRegionType]) => {
          if(l._7.equals(0)){
            out.collect((l._1, l._2, l._3, l._4, l._5, l._6))
          }
        })

    //OUTPUT
    filteredReduceResult
  }

  def assignRegionGroups(executor : FlinkImplementation, ds: DataSet[FlinkRegionType], groups: DataSet[(Long, Long)], binSize : Long, env : ExecutionEnvironment): DataSet[(Long, Long, Int, Int, Long, String, Long, Long, Char, Array[GValue], Long)] = {
    ds.join(groups).where(0).equalTo(0) {
      (region : FlinkRegionType, group : (Long, Long), out : Collector[(Long, Long, Int, Int, Long, String, Long, Long, Char, Array[GValue], Long)]) => {

        val s = new StringBuilder

        /*

        s.append(region._1.toString)
        s.append(group._2.toString)

        val hashId: Long =
          Hashing.md5().hashString(s.toString, Charsets.UTF_8).asLong()

        s.setLength(0)
        */

        val hashId : Long = Hashing.md5().newHasher().putLong(region._1).putLong(group._2).hash().asLong()


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
          out.collect((hashId, group._2, binStart, i, region._1, region._2, region._3, region._4, region._5, region._6, aggregationId))
        }

      }
    }
  }

  def assignExperimentGroups(executor : FlinkImplementation, ds: DataSet[FlinkRegionType], binSize : Long, env : ExecutionEnvironment): DataSet[(Int, Int, Long, String, Long, Long, Char, Array[GValue])] = {
    //ds.joinWithTiny(groups).where(0).equalTo(0){
    ds.flatMap((experiment : FlinkRegionType, out:Collector[(Int, Int, Long, String, Long, Long, Char, Array[GValue])]) => {
      //(experiment : FlinkRegionType, group : (Long, Long), out:Collector[(Int, Int, Long, String, Long, Long, Char, Array[GValue])]) => {
      val binStart = (experiment._3 / binSize).toInt
      val binEnd = (experiment._4 / binSize).toInt
      for (i <- binStart to binEnd) {
        out.collect((binStart, i, experiment._1, experiment._2, experiment._3, experiment._4, experiment._5, experiment._6))
      }
    }
    )
  }

  class PartialResult(val binnedRegion : (Long, Long, Int, Int, Long, String, Long, Long, Char, Array[GValue], Long), var count : Int){
    override def toString() : String = {
      "PR" + binnedRegion.toString() + " " + count
    }
  }

}