package it.polimi.genomics.flink.FlinkImplementation.operator.region

import it.polimi.genomics.core.DataStructures._
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.{GDouble, GValue}
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
object GenometricMap4 {

  final val logger = LoggerFactory.getLogger(this.getClass)

  @throws[SelectFormatException]
  def apply(executor : FlinkImplementation, grouping : OptionalMetaJoinOperator, aggregator : List[RegionAggregate.RegionsToRegion], reference : RegionOperator, experiments : RegionOperator, binSize : Long, env : ExecutionEnvironment) : DataSet[FlinkRegionType] = {

    //creating the datasets
    val ref: DataSet[(Long, String, Long, Long, Char, Array[GValue])] =
      executor.implement_rd(reference, env)
    val exp: DataSet[(Long, String, Long, Long, Char, Array[GValue])] =
      executor.implement_rd(experiments, env)


    execute(executor, grouping, aggregator, ref, exp, binSize, env)
  }

  @throws[SelectFormatException]
  def execute(executor : FlinkImplementation, grouping : OptionalMetaJoinOperator, aggregator : List[RegionAggregate.RegionsToRegion], ref: DataSet[(Long, String, Long, Long, Char, Array[GValue])], exp: DataSet[(Long, String, Long, Long, Char, Array[GValue])], binSize : Long, env : ExecutionEnvironment) : DataSet[FlinkRegionType] = {
    //creating the groups if they exist
    //otherwise create all possible (ref,exp) pairs
    val groups : DataSet[FlinkMetaJoinType] =
      executor.implement_mjd3(grouping, env)

    val pairs: DataSet[(Long, Long)] =
      {
        groups
          .join(groups).where(1).equalTo(1)
          .filter(x=>x._1._3==x._2._4).map(x=>(x._1._1,x._2._1)).distinct
      }


    //group the datasets
    val groupedRef: DataSet[(Long, Long, Int, Int, Long, String, Long, Long, Char, Array[GValue], Long)] =
      //assignRegionGroups(executor, ref.distinct(0,1,2,3,4), pairs, binSize, env)
      assignRegionGroups(executor,
        ref.distinct(x =>
          (x._1, x._2, x._3, x._4, x._5, x._6.map(_.toString).mkString("§"))),
        pairs, binSize, env)
    val groupedExp: DataSet[(Int, Int, Long, String, Long, Long, Char, Array[GValue])] =
      assignExperimentGroups(executor, exp, binSize, env)
    //ref(hashId, expId, binStart, bin, refid, chr, start, stop, strand, GValues, aggregationID)
    //exp(binStart, bin, expId, chr, start, stop., strand, GValues)

    //prepare a neutral extension of the array that matches the aggregator types
    //neutral means GInt(0), GDouble(0,0) and GString("")

    val extraData: List[Array[List[GValue]]] =
      List(
        exp.first(1).collect.map(_._6).headOption.getOrElse(new Array[GValue](0)).map(_ => List[GValue]())
      )
      //aggregator.map(_ => List[GValue]()).toArray
      //createSampleArrayExtension(aggregator, exp.first(1).collect.map(_._6).headOption.getOrElse(new Array[GValue](0)))

    //Join phase
    val coGroupResult : DataSet[(Long, String, Long, Long, Char, Array[GValue], Array[List[GValue]], Int, Long)] =
      groupedRef // expID, bin, chromosome
        .coGroup(groupedExp).where(1,3,5).equalTo(2,1,3){
        (references : Iterator[(Long, Long, Int, Int, Long, String, Long, Long, Char, Array[GValue], Long)], experiments : Iterator[(Int, Int, Long, String, Long, Long, Char, Array[GValue])], out : Collector[(Long, String, Long, Long, Char, Array[GValue], Array[List[GValue]], Int, Long)]) => {
          val refCollected : List[PartialResult] = references.map((r) => (new PartialResult(r, 0, extraData ))).toList
          for(e <- experiments){
            for(r <- refCollected){
              if(/* space overlapping */
                (r.binnedRegion._7 < e._6 && e._5 < r.binnedRegion._8)
                  && /* same strand */
                (r.binnedRegion._9.equals('*') || e._7.equals('*') || r.binnedRegion._9.equals(e._7))
                  && /* first comparison */
                (r.binnedRegion._3.equals(r.binnedRegion._4) ||  e._1.equals(e._2))
              ) {
                r.count += 1
                r.extra = r.extra :+ e._8.foldLeft(new Array[List[GValue]](0))((z : Array[List[GValue]], v : GValue) => z :+ List(v))
              }
            }
          }
          refCollected.foreach((pr) => {
            val res = (pr.binnedRegion._1, pr.binnedRegion._6, pr.binnedRegion._7, pr.binnedRegion._8, pr.binnedRegion._9, pr.binnedRegion._10, pr.extra.reduce((a,b) => a.zip(b).map((p) => p._1 ++ p._2)), pr.count, pr.binnedRegion._11)
            out.collect(res)
          })
        }
      }

    /*
    coGroupResult
      .groupBy(8)
      .combineGroup((a : Iterator[(Long, String, Long, Long, Char, Array[GValue], Array[List[GValue]], Int, Long)], out:Collector[Long]) => {
        while(a.hasNext){
          val l = a.next()
          if(l._9.equals(197188636128992227L)){
            println((l._1, l._2, l._3, l._4, l._5, l._6.mkString((" - ")), l._7.mkString(" - "), l._8, l._9 ))
          }
        }
      })*/

    //Aggregation phase
    val aggregationResult : DataSet[(Long, String, Long, Long, Char, Array[GValue])] =
      coGroupResult
        .groupBy(8)
        .reduceGroup((a : Iterator[(Long, String, Long, Long, Char, Array[GValue], Array[List[GValue]], Int, Long)], out:Collector[(Long, String, Long, Long, Char, Array[GValue], Array[List[GValue]], Int, Long)]) => {
          out.collect(
            a.reduce(
              (r1,r2) => {
                val out = (r1._1, r1._2, r1._3, r1._4, r1._5, r1._6,
                  r1._7
                    .zip(r2._7)
                    .map((a) => a._1 ++ a._2),
                  r1._8 + r2._8, r1._9)
                out
              }
            )
          )
        })
        //apply aggregation function on extra data
        .map((l : (Long, String, Long, Long, Char, Array[GValue], Array[List[GValue]], Int, Long)) => {
          val out =
            (l._1, l._2, l._3, l._4, l._5, l._6 ++ ( GDouble(l._8) +: {
              aggregator
                .map((f : RegionAggregate.RegionsToRegion) => {
                f.fun(l._7(f.index))
              })
            }))
          out
      })

    //OUTPU
//    println("aggregation "+aggregationResult.count)
    aggregationResult
  }

  /**
   * Create an extension array of the type consistent with the aggregator types
   * @param aggregator list of aggregator data
   * @return the extension array
   */
  /*
  def createSampleArrayExtension(aggregator : List[RegionAggregate.RegionsToRegion], sample : Array[GValue]) : Array[List[GValue]] = {
    //call the Helper using an array accumulator
    createSampleArrayExtensionHelper(aggregator, sample, List(List())).toArray
  }
  */

  /**
   * Helper to the extension creator
   * recursive function that
   * for each element of the array add one element of the respective type to the accumulator
   * if the set is empty return the accumulator
   * @param aggregator list of aggregator data
   * @param acc current accumulator
   * @return the extension array
   */
  /*
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
  */

  def assignRegionGroups(executor : FlinkImplementation, ds: DataSet[FlinkRegionType], groups: DataSet[(Long, Long)], binSize : Long, env : ExecutionEnvironment): DataSet[(Long, Long, Int, Int, Long, String, Long, Long, Char, Array[GValue], Long)] = {
    ds.joinWithTiny(groups).where(0).equalTo(0) {
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

  class PartialResult(val binnedRegion : (Long, Long, Int, Int, Long, String, Long, Long, Char, Array[GValue], Long), var count : Int, var extra : List[Array[List[GValue]]]){
    override def toString() : String = {
      "PR" + binnedRegion.toString() + " --" + count + "-- " + extra.mkString("-")
    }
  }

}