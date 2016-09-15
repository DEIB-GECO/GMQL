package it.polimi.genomics.spark.implementation.RegionsOperators.GenometricCover

import it.polimi.genomics.core.DataStructures.CoverParameters.CoverFlag.CoverFlag
import it.polimi.genomics.core.DataStructures.CoverParameters._
import it.polimi.genomics.core.DataStructures.RegionAggregate.RegionsToRegion
import it.polimi.genomics.core.DataStructures.{MetaGroupOperator, RegionOperator}
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.GRecordKey
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.{RangePartitioner, SparkContext}
import it.polimi.genomics.spark.implementation.RegionsOperators.GenometricCover.MergeRegions._
import scala.collection.Map
import scala.collection.immutable.HashMap

/**
* Created by abdulrahman Kaitoua on 24/06/15.
*/
object GenometricCover {
  final val BINNING_PARAMETER = 50000

  @throws[SelectFormatException]
  def apply(executor : GMQLSparkExecutor, coverFlag : CoverFlag, min : CoverParam, max : CoverParam, aggregators : List[RegionsToRegion], grouping : Option[MetaGroupOperator], inputDataset : RegionOperator, sc : SparkContext) : RDD[GRECORD] = {
    // CREATE DATASET RECURSIVELY
    val ds: RDD[GRECORD] =
      executor.implement_rd(inputDataset, sc)

    var allValue: Map[Long, Long] = Map()

    val gBroadcast = if(grouping.isDefined){
      val groups : RDD[FlinkMetaGroupType2] = executor.implement_mgd(grouping.get, sc)
      allValue = groups.groupBy(x=>x._2).countByKey()
      sc.broadcast(groups.collectAsMap())
    } else{
      allValue = Map(0l->ds.map(x=>x._1._1).distinct().count())
      sc.broadcast(Map(0l->0l))
    }

    val groupedDs: RDD[GRECORD_COVER] = assignGroups(executor, ds, gBroadcast, sc)

    val partitionedData: RDD[GRECORD_COVER] = new ShuffledRDD(groupedDs, new RangePartitioner[GRECORD_COVER_KEY,GRECORD_COVER_VALUE](5,groupedDs))

    val sortedParitionedData: RDD[GRECORD_COVER] = partitionedData.mapPartitions(x=>x.toArray.sortBy(s=>(s._1._1,s._1._2,s._1._3,s._1._4,s._2._1,s._2._2)).iterator)

    val minimum : Map[Long, Long] = min match{
        case ALL() => allValue
        case ANY() => Map(0L->1)
        case N(value) => Map(0L->value)
      }

    val maximum : Map[Long, Long] = max match{
        case ALL() => allValue
        case ANY() => Map((0L->Int.MaxValue.toLong))
        case N(value) => Map(0L->value.toLong)
      }
    import it.polimi.genomics.spark.implementation.RegionsOperators.GenometricCover.MergeRegions._
    val output: RDD[GRECORD_COVER] = coverFlag match {

      case CoverFlag.COVER => sortedParitionedData.mergeRegions(minimum,maximum,false).sortWithPreservedPartitioning().fuseIndexes()
      case CoverFlag.HISTOGRAM => sortedParitionedData.mergeRegions(minimum,maximum,false).sortWithPreservedPartitioning()
      case CoverFlag.SUMMIT => sortedParitionedData.mergeRegions(minimum,maximum,false).sortWithPreservedPartitioning().summit()
//      case CoverFlag.FLAT => coverHelper(0L, 0, 0, false, minimum, maximum, i, out)
    }

output.map(x=>(new GRecordKey(x._1._1,x._1._3,x._2._1,x._2._2,x._1._4),x._2._4))

/*
    // EXTRACT START-STOP POINTS
    val extracted: DataSet[(Long, Long, Char, String, Int, Int)] =
      extractor(groupedDs)

    //SPLIT DATASET -> FIND REGIONS THAT MAY CROSS A BIN
    val valid: DataSet[FlinkRegionType] =
      binnedPureCover.filter((v) => (v._3 % BINNING_PARAMETER != 0 && v._4 % BINNING_PARAMETER != 0))
    val notValid: DataSet[FlinkRegionType] =
      binnedPureCover.filter((v) => (v._3 % BINNING_PARAMETER == 0 || v._4 % BINNING_PARAMETER == 0))

    // JOIN REGIONS THAT
    // ARE IN THE SAME CHROMOSOME
    // HAVE COINCIDENT STOP AND START
    // HAVE SAME INTERSECTION COUNTER
    val joined : DataSet[FlinkRegionType] =
      notValid
        .groupBy(1)
        .sortGroup(2, Order.ASCENDING)
        .reduceGroup((i : Iterator[FlinkRegionType], out : Collector[FlinkRegionType]) => {
          if(i.hasNext) {
            var old: FlinkRegionType =
              i.next()

            while (i.hasNext) {
              val current = i.next()
              if (old._4.equals(current._3) && old._6(0).asInstanceOf[GDouble].v.equals(current._6(0).asInstanceOf[GDouble].v)){
                old = (old._1, old._2, old._3, current._4, old._5, old._6)
              } else {
                out.collect(old)
                old = current
              }
            }
            out.collect(old)
          }
        })

    val pureCover : DataSet[FlinkRegionType] =
      joined.union(valid)

    // USE GENOMETRIC MAP TO CALCULATE AGGREGATION
    // calculate aggregation
    val flat : Boolean =
      coverFlag.equals(CoverFlag.FLAT)

    val aggregated : DataSet[FlinkRegionType] =
      GMapForCover(aggregators, flat, pureCover, groupedDs)

    aggregated
*/
  }

  def assignGroups(executor : GMQLSparkExecutor, dataset : RDD[GRECORD], grouping : Broadcast[Map[Long, Long]], sc : SparkContext): RDD[GRECORD_COVER] = {
    dataset.flatMap{record=>
      var res = List[GRECORD_COVER]()
      val startbin = (Math.ceil(record._1._3 +1)/ BINNING_PARAMETER).toInt
      val stopbin = (Math.ceil(record._1._4 +1) / BINNING_PARAMETER).toInt
      val g: Long = grouping.value.get(record._1._1).getOrElse(0l)
      for (i <- startbin to stopbin) {
        if (record._1._5.equals('*')) {
          // the key: (groupID, Bin, Chrm, str), value : (start,stop,startbin,values())
          res ::=((g, i, record._1._2, '-'), (record._1._3, record._1._4, startbin, record._2))
          res ::=((g, i, record._1._2, '+'), (record._1._3, record._1._4, startbin, record._2))
        } else {
          res ::=((g, i, record._1._2, record._1._5),( record._1._3, record._1._4, startbin, record._2))
        }
      }
      res
    }
  }

}
