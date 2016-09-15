package it.polimi.genomics.flink.FlinkImplementation.operator.region

import it.polimi.genomics.core.DataStructures.RegionAggregate
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.{GDouble, GString, GValue}
import org.apache.flink.api.scala._
import org.apache.flink.hadoop.shaded.com.google.common.base.Charsets
import org.apache.flink.hadoop.shaded.com.google.common.hash.Hashing
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory


/**
 * Created by michelebertoni on 13/05/15.
 */
object GMapForCover {

  final val logger = LoggerFactory.getLogger(this.getClass)
  private final val BINNING_PARAMETER = GenometricCover.BINNING_PARAMETER

  def apply(aggregator : List[RegionAggregate.RegionsToRegion], flat : Boolean, ref : DataSet[FlinkRegionType], exp : DataSet[(Long, Int, Long, String, Long, Long, Char, Int, Array[GValue])]) : DataSet[FlinkRegionType] = {

    //group the dataset
    val groupedRef : DataSet[(Long, Int, Int, String, Long, Long, Char, Array[GValue], Long)] =
      assignRegionGroups(ref).distinct(8)

    //ref(id, binstart, bin, chr, start, stop, strand, array, aggregationID)
    //exp(id, bin, originalId, chr, start, stop, strand, binStart, array)

    //prepare a neutral extension of the array that matches the aggregator types
    //neutral means GInt(0), GDouble(0,0) and GString("")
    val extraData: Array[List[GValue]] =
      createSampleArrayExtension(aggregator,
        exp
          .first(1)
          .collect
          .map(_._9)
          .headOption
          .getOrElse(new Array[GValue](0))
      )

    //Join phase
    val coGroupResult : DataSet[(Long, String, Long, Long, Char, Array[GValue], Array[List[GValue]], Int, Long, Long, Long, Long, Long)] =
      groupedRef//id, bin, chromosome
        .coGroup(exp).where(0,2,3).equalTo(0,1,3){
        (l : Iterator[(Long, Int, Int, String, Long, Long, Char, Array[GValue], Long)], r : Iterator[(Long, Int, Long, String, Long, Long, Char, Int, Array[GValue])], out : Collector[(Long, String, Long, Long, Char, Array[GValue], Array[List[GValue]], Int, Long, Long, Long, Long, Long)]) => {
          val rCollected = r.toSet
          for(region <- l){
            var count = 0
            for(experiment <- rCollected){
              if(/* cross */
              /* space overlapping */
                (region._5 < experiment._6 && experiment._5 < region._6)
                  && /* same strand */ {
                  region._7 match {
                    case '*' => true
                    case '+' => experiment._7.equals('*') || experiment._7.equals('+')
                    case '-' => experiment._7.equals('*') || experiment._7.equals('-')
                  }
                }
                  && /* first comparison */
                  (region._2.equals(region._3) ||  experiment._2.equals(experiment._8))
              ) {
                val combinedArray : Array[List[GValue]] =
                  experiment._9.foldLeft(new Array[List[GValue]](0))((z : Array[List[GValue]], v : GValue) => z :+ List(v))
                count = count + 1;
                out.collect((region._1, region._4, region._5, region._6, region._7, region._8, combinedArray, 1, region._9, experiment._5, experiment._6, experiment._5, experiment._6))
              }
            }
            if(count == 0){
              out.collect((region._1, region._4, region._5, region._6, region._7, region._8, extraData, 0, region._9, 0L, 0L, 0L, 0L))
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
          (r1,r2) => {
            val startMin = Math.min(r1._10, r2._10)
            val startMax = Math.max(r1._10, r2._10)
            val endMin = Math.min(r1._11, r2._11)
            val endMax = Math.max(r1._11, r2._11)
            (r1._1, r1._2, r1._3, r1._4, r1._5, r1._6,
              r1._7
                .zip(r2._7)
                .map((a) => a._1 ++ a._2),
              r1._8 + r2._8, r1._9, startMin, endMax, if(startMax < endMin) startMax else 0L, if(endMin > startMax) endMin else 0L)
          }
        )
        //apply aggregation function on extra data
        .map((l : (Long, String, Long, Long, Char, Array[GValue], Array[List[GValue]], Int, Long, Long, Long, Long, Long)) => {
          val start : Double = if(flat) l._10 else l._3
          val end : Double = if (flat) l._11 else l._4
          (l._1, l._2, start.toLong, end.toLong, l._5,
            ((l._6 :+  GDouble(l._8)) ++
            aggregator
              .map((f : RegionAggregate.RegionsToRegion) => {
                f.fun(l._7(f.index))
              }))
              // Jaccard 1
              :+ { if(l._11-l._10 != 0){ GDouble(Math.abs((end-start)/(l._11-l._10))) } else { GDouble(0) } }
              // Jaccard 2
              :+ { if(l._11-l._10 != 0){ GDouble(Math.abs((l._13-l._12)/(l._11-l._10))) } else { GDouble(0) } }
          )
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

  def assignRegionGroups(ds: DataSet[FlinkRegionType]): DataSet[(Long, Int, Int, String, Long, Long, Char, Array[GValue], Long)] = {
    ds.flatMap((region : FlinkRegionType, out : Collector[(Long, Int, Int, String, Long, Long, Char, Array[GValue], Long)]) => {
        val aggregationId: Long =
          Hashing.md5().hashString((region._1.toString + region._2.toString + region._3.toString + region._4.toString + region._5.toString + region._6.mkString("§")).toString, Charsets.UTF_8).asLong()

        val binStart = (region._3 / BINNING_PARAMETER).toInt
        val binEnd = (region._4 / BINNING_PARAMETER).toInt
        for (i <- binStart to binEnd) {
          out.collect((region._1, binStart, i, region._2, region._3, region._4, region._5, region._6, aggregationId))
        }

      }
    )
  }

}