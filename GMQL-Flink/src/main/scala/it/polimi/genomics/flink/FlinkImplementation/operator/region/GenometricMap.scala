package it.polimi.genomics.flink.FlinkImplementation.operator.region

import it.polimi.genomics.core.DataStructures.{MetaJoinOperator, RegionAggregate, RegionOperator}
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.{GString, GDouble, GValue}
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.flink.FlinkImplementation.FlinkImplementation
import org.apache.flink.api.scala._
import org.apache.flink.hadoop.shaded.com.google.common.base.Charsets
import org.apache.flink.hadoop.shaded.com.google.common.hash.Hashing
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory


/**
 * Created by michelebertoni on 13/05/15.
 */
object GenometricMap {
  final val logger = LoggerFactory.getLogger(this.getClass)

  private final val BINNING_PARAMETER = 50000


  @throws[SelectFormatException]
  def apply(executor : FlinkImplementation, grouping : Option[MetaJoinOperator], aggregator : List[RegionAggregate.RegionsToRegion], reference : RegionOperator, experiments : RegionOperator, env : ExecutionEnvironment) : DataSet[FlinkRegionType] = {

    //creating the datasets
    val ref: DataSet[(Long, String, Long, Long, Char, Array[GValue])] =
      executor.implement_rd(reference, env)
    val exp: DataSet[(Long, String, Long, Long, Char, Array[GValue])] =
      executor.implement_rd(experiments, env)

    execute(executor, grouping, aggregator, ref, exp, env)
  }

  @throws[SelectFormatException]
  def execute(executor : FlinkImplementation, grouping : Option[MetaJoinOperator], aggregator : List[RegionAggregate.RegionsToRegion], ref: DataSet[(Long, String, Long, Long, Char, Array[GValue])], exp: DataSet[(Long, String, Long, Long, Char, Array[GValue])], env : ExecutionEnvironment) : DataSet[FlinkRegionType] = {
    //creating the groups if they exist
    //otherwise create all possible (ref,exp) pairs
    val groups: DataSet[(Long, Long)] =
      if(grouping.isDefined){
        executor.implement_mjd(grouping.get, env)
      } else {
        ref.map(_._1).distinct((t) => t).cross(exp.map(_._1).distinct((t) => t))
      }

    //group the datasets
    val groupedRef: DataSet[(Long, Long, Int, Int, Long, String, Long, Long, Char, Array[GValue], Long)] =
      assignRegionGroups(executor, ref, groups, env).distinct
    val groupedExp: DataSet[(Int, Int, Long, String, Long, Long, Char, Array[GValue])] =
      assignExperimentGroups(executor, exp, groups, env)

    //ref(hashId, expId, binStart, bin, refid, chr, start, stop, strand, GValues, aggregationID)
    //exp(binStart, bin, expId, chr, start, stop., strand, GValues)

    //prepare a neutral extension of the array that matches the aggregator types
    //neutral means GInt(0), GDouble(0,0) and GString("")
    val extraData: Array[List[GValue]] =
      createSampleArrayExtension(aggregator, exp.first(1).collect.map(_._6).head)

    //Join phase
    val coGroupResult : DataSet[(Long, String, Long, Long, Char, Array[GValue], Array[List[GValue]], Int, Long)] =
      groupedRef//expId, bin, chromosome
        .coGroup(groupedExp).where(1,3,5).equalTo(2,1,3){
        (l : Iterator[(Long, Long, Int, Int, Long, String, Long, Long, Char, Array[GValue], Long)], r : Iterator[(Int, Int, Long, String, Long, Long, Char, Array[GValue])], out : Collector[(Long, String, Long, Long, Char, Array[GValue], Array[List[GValue]], Int, Long)]) => {
          val rCollected = r.toSet
          for(region <- l){
            var count = 0
            for(experiment <- rCollected){
              if(/* cross */
              /* space overlapping */
                (region._7 < experiment._6 && experiment._5 < region._8)
                  && /* same strand */ {
                  region._9 match {
                    case '*' => true
                    case '+' => experiment._7.equals('*') || experiment._7.equals('+')
                    case '-' => experiment._7.equals('*') || experiment._7.equals('-')
                  }
                }
                  && /* first comparison */
                  (region._3.equals(region._4) ||  experiment._1.equals(experiment._2))
              ) {
                val combinedArray : Array[List[GValue]] =
                  experiment._8.foldLeft(new Array[List[GValue]](0))((z : Array[List[GValue]], v : GValue) => z :+ List(v))
                count = count + 1;
                out.collect((region._1, region._6, region._7, region._8, region._9, region._10, combinedArray, 1, region._11))
              }
            }
            if(count == 0){
              out.collect((region._1, region._6, region._7, region._8, region._9, region._10, extraData, 0, region._11))
            }
          }
        }
      }

    //Aggregation phase
    val aggregationResult: DataSet[(Long, String, Long, Long, Char, Array[GValue])] =
      coGroupResult
        .groupBy(8)
        //reduce phase
        //concatenation of extra data
        .reduce(
          (r1,r2) =>
            (r1._1, r1._2, r1._3, r1._4, r1._5, r1._6,
              r1._7
                .zip(r2._7)
                .map((a) => a._1 ++ a._2),
              r1._8 + r2._8, r1._9)
        )
        //apply aggregation function on extra data
        .map((l : (Long, String, Long, Long, Char, Array[GValue], Array[List[GValue]], Int, Long)) => {
        (l._1, l._2, l._3, l._4, l._5, l._6 ++ ( GDouble(l._8) +: {
          aggregator
            .map((f : RegionAggregate.RegionsToRegion) => {
            f.fun(l._7(f.index))
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
      }
    }
  }

  def assignRegionGroups(executor : FlinkImplementation, ds: DataSet[FlinkRegionType], groups: DataSet[(Long, Long)], env : ExecutionEnvironment): DataSet[(Long, Long, Int, Int, Long, String, Long, Long, Char, Array[GValue], Long)] = {
    ds.join(groups).where(0).equalTo(0) {
      (region : FlinkRegionType, pair : (Long, Long), out : Collector[(Long, Long, Int, Int, Long, String, Long, Long, Char, Array[GValue], Long)]) => {
        val hashId: Long =
          Hashing.md5().hashString((region._1.toString + pair._2.toString).toString, Charsets.UTF_8).asLong()

        val aggregationId: Long =
          Hashing.md5().hashString((hashId.toString + region._2.toString + region._3.toString + region._4.toString + region._5.toString + region._6.mkString("§")).toString, Charsets.UTF_8).asLong()

        val binStart = (region._3 / BINNING_PARAMETER).toInt
        val binEnd = (region._4 / BINNING_PARAMETER).toInt
        for (i <- binStart to binEnd) {
          out.collect((hashId, pair._2, binStart, i, region._1, region._2, region._3, region._4, region._5, region._6, aggregationId))
        }

      }
    }
  }

  def assignExperimentGroups(executor : FlinkImplementation, ds: DataSet[FlinkRegionType], groups: DataSet[(Long, Long)], env : ExecutionEnvironment): DataSet[(Int, Int, Long, String, Long, Long, Char, Array[GValue])] = {
    ds.flatMap((e, out:Collector[(Int, Int, Long, String, Long, Long, Char, Array[GValue])]) => {
      val binStart = (e._3 / BINNING_PARAMETER).toInt
      val binEnd = (e._4 / BINNING_PARAMETER).toInt
      for (i <- binStart to binEnd) {
        out.collect((binStart, i, e._1, e._2, e._3, e._4, e._5, e._6))
      }
    })
  }

}