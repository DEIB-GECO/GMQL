package it.polimi.genomics.flink.FlinkImplementation.operator.region

import it.polimi.genomics.core.DataStructures.{MetaJoinOperator, RegionAggregate, RegionOperator}
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.{GDouble, GString, GValue}
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
object GenometricMap2 {

  final val logger = LoggerFactory.getLogger(this.getClass)
  private final val BINNING_PARAMETER = 100000


  @throws[SelectFormatException]
  def apply(executor : FlinkImplementation, grouping : Option[MetaJoinOperator], aggregator : List[RegionAggregate.RegionsToRegion], reference : RegionOperator, experiments : RegionOperator, env : ExecutionEnvironment) : DataSet[FlinkRegionType] = {

    //grouped dataset
    val ref: DataSet[(String, Int, Int, Long, String, Long, Long, Char, Array[GValue])] =
      assignGroups(executor, executor.implement_rd(reference, env), grouping, env).distinct
    val exp: DataSet[(String, Int, Int, Long, String, Long, Long, Char, Array[GValue])] =
      assignGroups(executor, executor.implement_rd(experiments, env), grouping, env)

    //SECTION 1
    //join

    //Join phase
    //cost O( |ref| * |exp| )
    val joinResult : DataSet[(Long, String, Long, Long, Char, Array[GValue], Array[List[GValue]], Int, String)] =
      ref//groupId, bin, chromosome
        .joinWithHuge(exp).where(0,2,4).equalTo(0,2,4){
        (r : (String, Int, Int, Long, String, Long, Long, Char, Array[GValue]), x : (String, Int, Int, Long, String, Long, Long, Char, Array[GValue]), out : Collector[(Long, String, Long, Long, Char, Array[GValue], Array[List[GValue]], Int, String)]) => {
          if(/* cross */
          /* space overlapping */
            (r._6 < x._7 && x._6 < r._7)
              && /* same strand */ {
              r._8 match {
                case '*' => true
                case '+' => x._8.equals('*') || x._8.equals('+')
                case '-' => x._8.equals('*') || x._8.equals('-')
              }
            }
              && /* first comparison */
              (r._2.equals(r._3) ||  x._2.equals(x._3))
          ) {

            val hashId : Long =
              Hashing.md5().hashString((r._4.toString + x._4.toString).toString, Charsets.UTF_8).asLong()


            val aggregationId: String =
              (hashId.toString + r._5.toString + r._6.toString + r._7.toString + r._8.toString + r._9.mkString("§")).toString

            val combinedArray : Array[List[GValue]] =
              x._9.foldLeft(new Array[List[GValue]](0))((z : Array[List[GValue]], v : GValue) => z :+ List(v))

            out.collect(hashId, r._5, r._6, r._7, r._8, r._9, combinedArray, 1, aggregationId)
          }
        }
      }

    //joinResult.print

    //Aggregation phase over experiment attribute
    //cost O( sort(join) + 2*join + aggregation cost)
    val aggregationResult: DataSet[(Long, String, Long, Long, Char, Array[GValue], List[GValue], String)] =
      joinResult
        .groupBy(8)
        .reduce(
          (r1,r2) =>
            (r1._1, r1._2, r1._3, r1._4, r1._5, r1._6,
              r1._7
                .zip(r2._7)
                .map((a) => a._1 ++ a._2),
              r1._8 + r2._8, r1._9)
        )
        .map((l : (Long, String, Long, Long, Char, Array[GValue], Array[List[GValue]], Int, String)) => {
        (l._1, l._2, l._3, l._4, l._5, l._6, GDouble(l._8) +: {
          //val grid = swap(l._7)
          aggregator
            .map((f : RegionAggregate.RegionsToRegion) => {
            f.fun(l._7(f.index))
          })
        }, l._9)
      })

    //logger.error("---------------------- res count " + aggregationResult.count)
    //aggregationResult.print

    //aggregationResult.print



    //SECTION 2
    //left (ref) outer join

    //Prepare a set of reference with count = 0 and null extra value
    //it will be added to the join result to complete the outer join on reference

    //prepare a neutral extension of the array that matches the aggregator types
    //neutral means GInt(0), GDouble(0,0) and GString("")
    val extraData =
      createSampleArrayExtension(aggregator, exp.first(1).collect.map(_._9).head)

    //extract a list of experiment ids
    /*
    val expGroupIdSampleId =
      exp
        .map((v) => (v._4))
        .distinct((t) => t)
    */

    val expGroupIdSampleId =
      exp
        .map((v) => (v._1, v._4))
        .distinct(0,1)

    //expGroupIdSampleId.print

    //prepare the reference set with the extra null data
    /*
    val referenceCross: DataSet[(Long, String, Long, Long, Char, Array[GValue], List[GValue])] =
      ref.crossWithTiny(expGroupIdSampleId){(l,r) =>
        (Hashing.md5().hashString((l._4.toString + r.toString).toString, Charsets.UTF_8).asLong(), l._5, l._6, l._7, l._8, l._9, extraData)
      }
    */
    val referenceCross: DataSet[(Long, String, Long, Long, Char, Array[GValue], List[GValue], String)] =
      ref.joinWithTiny(expGroupIdSampleId).where(0).equalTo(0){(l,r) =>{
        val hashId =
          Hashing.md5().hashString((l._4.toString + r._2.toString).toString, Charsets.UTF_8).asLong()

        val aggregationId: String =
          (hashId.toString + l._5.toString + l._6.toString + l._7.toString + l._8.toString + l._9.mkString("§")).toString

        (hashId, l._5, l._6, l._7, l._8, l._9, extraData, aggregationId)
      }
      }


    //remove from sample reference all the matched element
    //cost O( sort(res) + sort(join) + |res| + |join| ) ----- error
    //cost O( hash(res + join) + |res| + |join| )
    /*
        val referenceCrossReduced =
          referenceCross.coGroup(aggregationResult)
            .where((v) => {val s = new StringBuilder; s.append(v._1).append(v._2).append(v._3).append(v._4).append(v._5).append(extract(v._6)).toString()})
            .equalTo((v) => {val s = new StringBuilder; s.append(v._1).append(v._2).append(v._3).append(v._4).append(v._5).append(extract(v._6)).toString()}){
            (region, matchedid, out: Collector[(Long, String, Long, Long, Char, Array[GValue], List[GValue])]) =>
             if(matchedid.size.equals(0)){
               region.foreach((r) => out.collect(r))
             }
          }
    */


    //merge the mapped data with the sample reference
    //aggregationResult
    //.union(referenceCrossReduced)


    //cost O( sort(res+join) + |res| + |join| + mergingAttributes )
    val temp = aggregationResult
      .union(referenceCross)

    //temp.print

    val outerJoinMap =
      temp
        .groupBy(7)
        .reduce((a, b) => {
        (a._1, a._2, a._3, a._4, a._5, a._6, mergeSeqArray(a._7, b._7), a._8)
      })
        .map((r) => (r._1, r._2, r._3, r._4, r._5, r._6 ++ r._7))



    //OUTPUT
    outerJoinMap
  }

  /**
   * Create an extension array of the type consistent with the aggregator types
   * @param aggregator list of aggregator data
   * @return the extension array
   */
  def createSampleArrayExtension(aggregator : List[RegionAggregate.RegionsToRegion], sample : Array[GValue]) : List[GValue] = {
    //call the Helper using an array accumulator that contains the first value: it is the count of element as 0
    createSampleArrayExtensionHelper(aggregator, sample, List(GDouble(0)))
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
  def createSampleArrayExtensionHelper(aggregator : List[RegionAggregate.RegionsToRegion], sample : Array[GValue], acc : List[GValue]) : List[GValue] = {
    if(aggregator.size.equals(0)){
      acc
    } else {
      sample(aggregator(0).index) match{
        //case GInt(_) => createSampleArrayExtensionHelper(aggregator.drop(1), sample, acc :+ GInt(0))
        case GDouble(_) => createSampleArrayExtensionHelper(aggregator.drop(1), sample, acc :+ GDouble(0))
        case GString(_) => createSampleArrayExtensionHelper(aggregator.drop(1), sample, acc :+ GString(""))
      }
    }
  }


  /**
   * Merge two list of region attribute, keeps the values that are non null
   * @param a first list
   * @param b second list
   * @return merged list
   */
  def mergeSeqArray(a : List[GValue],b : List[GValue]) : List[GValue] = {
    if(a.size > b.size){
      a
    } else if (a.size < b.size){
      b
    } else {
      mergeSeqArrayHelper(a, b, List[GValue]())
    }
  }

  /**
   * Recursive function that execute the list mergin
   *
   * @param a first list
   * @param b second list
   * @param acc accumulator
   * @return merged list
   */
  def mergeSeqArrayHelper(a : List[GValue], b : List[GValue], acc : List[GValue]) : List[GValue] = {
    if(a.size.equals(0)){
      acc
    } else {
      a(0) match {
        case GDouble(v) => {
          mergeSeqArrayHelper(a.drop(1), b.drop(1), acc :+ GDouble(a(0).asInstanceOf[GDouble].v + b(0).asInstanceOf[GDouble].v))
        }
        case GString(v) => {
          mergeSeqArrayHelper(a.drop(1), b.drop(1), acc :+ GString(a(0).asInstanceOf[GString].v + b(0).asInstanceOf[GString].v))
        }
      }
    }
  }

  /**
   * Assigns each region of the dataset to a group wrt to the grouping phase of MetaJoin and creates bins based on the chromosome, groupId and start position
   * In case a region is longer than a bin it is duplicated for each bin
   * @param executor instance of the FlinkImplementation
   * @param ds input dataset
   * @param groups group of sample resulting from MetaJoin
   * @param env execution environment
   * @return a dataset of region with all the fields required for binning and grouping (groupID, binStart, bin, sampleId, chromosome, start, stop, strand, array of GValue)
   */
  def assignGroups(executor : FlinkImplementation, ds: DataSet[FlinkRegionType], groups: Option[MetaJoinOperator], env : ExecutionEnvironment): DataSet[(String, Int, Int, Long, String, Long, Long, Char, Array[GValue])] = {
    if(groups.isDefined) {
      ds.joinWithTiny(executor.implement_mjd2(groups.get, env)).where(0).equalTo(0) {
        (r: FlinkRegionType, l: (FlinkMetaJoinType2), out: Collector[(String, Int, Int, Long, String, Long, Long, Char, Array[GValue])]) => {
          l._2.map((id: Long) => {
            val binStart = (r._3 / BINNING_PARAMETER).toInt
            val binEnd = (r._4 / BINNING_PARAMETER).toInt
            for (i <- binStart to binEnd) {
              out.collect(
                (id.toString /*+ r._2*/, binStart, i, r._1, r._2, r._3, r._4, r._5, r._6)
              )
            }
          })
        }
      }
    } else {
      ds.flatMap((r: FlinkRegionType, out: Collector[(String, Int, Int, Long, String, Long, Long, Char, Array[GValue])]) => {
        val binStart = (r._3/BINNING_PARAMETER).toInt
        val binEnd = (r._4/BINNING_PARAMETER).toInt
        for(i <- binStart to binEnd){
          out.collect(
            ("1", binStart, i, r._1, r._2, r._3, r._4, r._5, r._6)
          )
        }
      })
    }
  }

}