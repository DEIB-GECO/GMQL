package it.polimi.genomics.flink.FlinkImplementation.operator.region

import it.polimi.genomics.core.DataStructures.RegionAggregate
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.{GDouble, GValue}
import org.apache.flink.api.scala._
import org.apache.flink.hadoop.shaded.com.google.common.base.Charsets
import org.apache.flink.hadoop.shaded.com.google.common.hash.Hashing
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory


/**
 * Created by michelebertoni on 13/05/15.
 */
object GMapForCover3 {

  final val logger = LoggerFactory.getLogger(this.getClass)
  //private final val BINNING_PARAMETER = GenometricCover.BINNING_PARAMETER

  def apply(aggregator : List[RegionAggregate.RegionsToRegion], flat : Boolean, ref : DataSet[FlinkRegionType], exp : DataSet[(Long, String, Long, Long, Char, Long, Array[GValue])], binSize : Long) : DataSet[FlinkRegionType] = {

    //group the dataset
    val groupedRef : DataSet[(Long, Int, Int, String, Long, Long, Char, Array[GValue], Long)] =
      assignRegionGroups(ref, binSize).distinct(0,1,2,3,4,5,6,8)


    val binnedExp =
      assignBinExp(exp, binSize)
    
    //Join phase
    val coGroupResult : DataSet[(Long, String, Long, Long, Char, Array[GValue], Array[List[GValue]], Int, Long, Long, Long, Long, Long)] =
      groupedRef//id, bin, chromosome
        .coGroup(binnedExp).where(0,2,3).equalTo(0,8,1){
        (references : Iterator[(Long, Int, Int, String, Long, Long, Char, Array[GValue], Long)], experiments : Iterator[(Long, String, Long, Long, Char, Long, Array[GValue], Int, Int)], out : Collector[(Long, String, Long, Long, Char, Array[GValue], Array[List[GValue]], Int, Long, Long, Long, Long, Long)]) => {

          val refCollected = references.toList.map((v) => new PartialResult(v, 0, List[Array[List[GValue]]](), -1L, -1L, -1L, -1L))
          for (e <- experiments){
            for(r <- refCollected){
              if(/* cross */
              /* space overlapping */
                (r.binnedRegion._5 < e._4 && e._3 < r.binnedRegion._6)
                  && /* same strand */
                  (r.binnedRegion._7.equals('*') || e._5.equals('*') || r.binnedRegion._7.equals(e._5))
                  && /* first comparison */
                  (r.binnedRegion._2.equals(r.binnedRegion._3) ||  e._8.equals(e._9))
              ) {
                r.count += 1
                r.extra = r.extra :+ e._7.foldLeft(new Array[List[GValue]](0))((z : Array[List[GValue]], v : GValue) => z :+ List(v))

                val startMin =
                  if(r.startUnion.equals(-1L)){
                    e._3
                  } else if(e._3.equals(-1L)){
                    r.startUnion
                  } else {
                    Math.min(r.startUnion, e._3)
                  }

                val endMax =
                  Math.max(r.endUnion, e._4)

                val startMax =
                  Math.max(r.startIntersection, e._3)

                val endMin =
                  if(r.endIntersection.equals(-1L)){
                    e._4
                  } else if(e._4.equals(-1L)){
                    r.endIntersection
                  } else {
                    Math.min(r.endIntersection, e._4)
                  }

                r.startUnion = startMin
                r.endUnion = endMax
                r.startIntersection = startMax
                r.endIntersection = endMin

              }
            }
          }
          refCollected.foreach((pr) => {
            //println(pr)
            out.collect((pr.binnedRegion._1, pr.binnedRegion._4, pr.binnedRegion._5, pr.binnedRegion._6, pr.binnedRegion._7, pr.binnedRegion._8, pr.extra.reduceOption((a,b) => a.zip(b).map((p) => p._1 ++ p._2)).getOrElse(new Array[List[GValue]](0)), pr.count, pr.binnedRegion._9, pr.startUnion, pr.endUnion, pr.startIntersection, pr.endIntersection))
          })

        }
      }//.withForwardedFieldsFirst("0;3->1;4->2;5->3;6->4;7->5")

    //Aggregation phase
    val aggregationResult: DataSet[(Long, String, Long, Long, Char, Array[GValue])] =
      coGroupResult
        .groupBy(8)
        //reduce phase
        //concatenation of extra data
        .reduce(
          (r1,r2) => {
            val startMin =
              if(r1._10.equals(-1L)){
                r2._10
              } else if(r2._10.equals(-1L)){
                r1._10
              } else {
                Math.min(r1._10, r2._10)
              }

            val endMax =
              Math.max(r1._11, r2._11)

            val startMax =
              Math.max(r1._12, r2._12)

            val endMin =
              if(r1._13.equals(-1L)){
                r2._13
              } else if(r2._13.equals(-1L)){
                r1._13
              } else {
                Math.min(r1._13, r2._13)
              }


            val extra =
              if(r1._7.isEmpty){
                r2._7
              } else if(r2._7.isEmpty){
                r1._7
              } else {
                r1._7
                  .zip(r2._7)
                  .map((a) => a._1 ++ a._2)
              }

            (r1._1, r1._2, r1._3, r1._4, r1._5, r1._6, extra,
              r1._8 + r2._8, r1._9, startMin, endMax, if(startMax < endMin) startMax else 0L, if(endMin > startMax) endMin else 0L)
          }
        )//.withForwardedFields("0;1;2;3;4;5;8")
        //apply aggregation function on extra data
        .map((l : (Long, String, Long, Long, Char, Array[GValue], Array[List[GValue]], Int, Long, Long, Long, Long, Long)) => {
          val start : Double = if(flat) l._10 else l._3
          val end : Double = if (flat) l._11 else l._4


          (l._1, l._2, start.toLong, end.toLong, l._5,
            ((l._6 /* :+  GDouble(l._8)*/) ++
            aggregator
              .map((f : RegionAggregate.RegionsToRegion) => {
                f.fun(if(l._7.length > f.index) l._7(f.index) else List())
              }))
              // Jaccard 1
              :+ { if(l._11-l._10 != 0){ GDouble((end.toDouble-start)/(l._11-l._10)) } else { GDouble(0) } }
              // Jaccard 2
              :+ { if(l._11-l._10 != 0 && l._13-l._12 >= 0){ GDouble((l._13.toDouble-l._12)/(l._11-l._10)) } else { GDouble(0) } }
          )
        })//.withForwardedFields("0;1;4")

    //OUTPUT
    aggregationResult
  }


  def assignRegionGroups(ds : DataSet[FlinkRegionType], binSize : Long): DataSet[(Long, Int, Int, String, Long, Long, Char, Array[GValue], Long)] = {
    ds.flatMap((region : FlinkRegionType, out : Collector[(Long, Int, Int, String, Long, Long, Char, Array[GValue], Long)]) => {
        val aggregationId: Long =
          Hashing.md5().hashString((region._1.toString + region._2.toString + region._3.toString + region._4.toString + region._5.toString + region._6.mkString("§")).toString, Charsets.UTF_8).asLong()

        val binStart = (region._3 / binSize).toInt
        val binEnd = (region._4 / binSize).toInt
        for (i <- binStart to binEnd) {
          out.collect((region._1, binStart, i, region._2, region._3, region._4, region._5, region._6, aggregationId))
        }

      }
    ).withForwardedFields("0;1->3;2->4;3->5;4->6;5->7")
  }

  def assignBinExp(ds : DataSet[(Long, String, Long, Long, Char, Long, Array[GValue])], binSize : Long) = {
    ds.flatMap((region, out : Collector[(Long, String, Long, Long, Char, Long, Array[GValue], Int ,Int)]) => {
      val binStart = (region._3 / binSize).toInt
      val binEnd = (region._4 / binSize).toInt
      for (i <- binStart to binEnd) {
        out.collect((region._1, region._2, region._3, region._4, region._5, region._6, region._7, binStart, i))
      }
    }).withForwardedFields("0;1;2;3;4;5;6")
  }

  sealed case class PartialResult(binnedRegion : (Long, Int, Int, String, Long, Long, Char, Array[GValue], Long), var count : Int, var extra : List[Array[List[GValue]]], var startUnion : Long, var endUnion : Long, var startIntersection : Long, var endIntersection : Long)

}