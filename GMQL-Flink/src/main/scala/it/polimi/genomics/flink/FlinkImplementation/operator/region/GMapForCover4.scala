package it.polimi.genomics.flink.FlinkImplementation.operator.region

import it.polimi.genomics.core.DataStructures.RegionAggregate
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.{GDouble, GValue}
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.scala._
import org.apache.flink.hadoop.shaded.com.google.common.base.Charsets
import org.apache.flink.hadoop.shaded.com.google.common.hash.Hashing
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory


/**
  * Created by michelebertoni on 13/05/15.
  */
object GMapForCover4 {

  final val logger = LoggerFactory.getLogger(this.getClass)
  //private final val BINNING_PARAMETER = GenometricCover.BINNING_PARAMETER

  def apply(aggregator : List[RegionAggregate.RegionsToRegion],
            flat : Boolean,
            ref : DataSet[FlinkRegionType],
            exp : DataSet[(Long, String, Long, Long, Char, Long, Array[GValue])],
            binSize : Long) : DataSet[FlinkRegionType] = {

    //group the dataset
    val groupedRef : DataSet[(Long, Int, String, Long, Long, Char, Array[GValue], Long)] =
      assignRegionGroups(ref, binSize).distinct(1,7)


    val binnedExp =
      assignBinExp(exp, binSize)


    val joinRefExp =
      groupedRef.join(binnedExp, JoinHint.BROADCAST_HASH_FIRST).where(0,1,2).equalTo(0,7,1){
        (reference, experiment,
         out : Collector[(Long, String, Long, Long, Char, Array[GValue],Long, Long, Long, Long, Array[List[GValue]])]) => {
          val ref_start_bin = reference._4 / binSize
          val exp_start_bin = experiment._3 / binSize
          if (reference._2 == ref_start_bin || reference._2 ==exp_start_bin) {
            out.collect((reference._1, reference._3, reference._4, reference._5, reference._6, reference._7,
              experiment._3, experiment._4, experiment._3, experiment._4,
              experiment._7.foldLeft(new Array[List[GValue]](0))((z : Array[List[GValue]], v : GValue) => z :+ List(v))))
          }
        }
      }.groupBy(0,1,2,3,4)
        .reduce{ (l,r) => {
          (l._1,l._2,l._3,l._4,l._5,l._6, Math.min(l._7, r._7), Math.max(l._8, r._8), Math.max(l._9,r._9), Math.min(l._10,r._10), l._11++r._11)
        }}.map {
        r:(Long, String, Long, Long, Char, Array[GValue], Long, Long, Long, Long, Array[List[GValue]]) => {
          val jaccard1 = if(r._8-r._7 != 0) { GDouble((r._4.toDouble-r._3)/(r._8-r._7).toDouble)} else {GDouble(0)}
          val jaccard2 = if(r._8-r._7 != 0 && r._10-r._9 >= 0) {GDouble((r._10-r._9).toDouble / (r._8-r._7).toDouble)} else {GDouble(0)}
          (r._1, r._2, r._3, r._4, r._5, r._6 :+ jaccard1 :+ jaccard2 )
        }
      }

    joinRefExp
  }


  def assignRegionGroups(ds : DataSet[FlinkRegionType], binSize : Long): DataSet[(Long, Int, String, Long, Long, Char, Array[GValue], Long)] = {
    ds.flatMap((region : FlinkRegionType, out : Collector[(Long, Int,  String, Long, Long, Char, Array[GValue], Long)]) => {
      val aggregationId: Long =
        Hashing.md5().hashString((region._1.toString + region._2.toString + region._3.toString + region._4.toString + region._5.toString + region._6.mkString("§")).toString, Charsets.UTF_8).asLong()

      val binStart = (region._3 / binSize).toInt
      val binEnd = (region._4 / binSize).toInt
      for (i <- binStart to binEnd) {
        out.collect((region._1, i, region._2, region._3, region._4, region._5, region._6, aggregationId))
      }

    }
    ).withForwardedFields("0;1->2;2->3;3->4;4->5;5->6")
  }

  def assignBinExp(ds : DataSet[(Long, String, Long, Long, Char, Long, Array[GValue])], binSize : Long) = {
    ds.flatMap((region, out : Collector[(Long, String, Long, Long, Char, Long, Array[GValue], Int)]) => {
      val binStart = (region._3 / binSize).toInt
      val binEnd = (region._4 / binSize).toInt
      for (i <- binStart to binEnd) {
        out.collect((region._1, region._2, region._3, region._4, region._5, region._6, region._7, i))
      }
    }).withForwardedFields("0;1;2;3;4;5;6")
  }

  sealed case class PartialResult(
                                   binnedRegion : (Long, Int, Int, String, Long, Long, Char, Array[GValue], Long),
                                   var count : Int,
                                   var extra : List[Array[List[GValue]]],
                                   var startUnion : Long,
                                   var endUnion : Long,
                                   var startIntersection : Long,
                                   var endIntersection : Long)

}