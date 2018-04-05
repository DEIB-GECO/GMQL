package it.polimi.genomics.spark.implementation.RegionsOperators

import it.polimi.genomics.core.DataStructures._
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.GRecordKey
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

/**
 * Created by abdulrahman kaitoua on 04/07/15.
 */
object MergeRD {

  private final val logger = LoggerFactory.getLogger(this.getClass);

  @throws[SelectFormatException]
  def apply(operator: IROperator, executor : GMQLSparkExecutor, dataset : RegionOperator, groups : Option[MetaGroupOperator], sc : SparkContext) : RDD[GRECORD] = {
    logger.info("----------------Merge executing..")

    val ds : RDD[GRECORD] =
      executor.implement_rd(dataset, sc)

    val groupedDs : RDD[GRECORD] =
      if (groups.isDefined) {
        //group
        val grouping = executor.implement_mgd(groups.get, sc);
        assignGroups(ds, grouping)
      } else {
        //union of samples
        ds.map((r) => {
          (new GRecordKey(1L, r._1._2, r._1._3, r._1._4, r._1._5), r._2)
        })
      }

    if( operator.requiresOutputProfile && dataset.outputProfile.isDefined && groups.isEmpty ) {

      val (newNumRegions, newAverageLen, newVarianceLen) =  dataset.outputProfile.get.samples.map( x => {
        (x.get(Feature.NUM_REG), x.get(Feature.AVG_REG_LEN), x.get(Feature.VARIANCE_LENGTH)) }).reduce( (a,b) => {

        val (numReg_1, avgLen_1, v_1) = (a._1, a._2, a._3)
        val (numReg_2, avgLen_2, v_2) = (b._1, b._2, b._3)

        val nextCount = numReg_1 + numReg_2
        val nextAvg   = avgLen_1*(avgLen_1/nextCount) + avgLen_2*(avgLen_2/nextCount)
        val nextVar   = (numReg_1 / nextCount) * ( Math.pow(v_1,2) + Math.pow(avgLen_1 - nextAvg, 2) ) +
          (numReg_2 / nextCount) * ( Math.pow(v_2,2) + Math.pow(avgLen_2 - nextAvg, 2) )

        (nextCount, nextAvg, nextVar)

      })

      val mergedSamples = dataset.outputProfile.get.samples.reduce( (x,y) => {

        val newSample = new GMQLSampleStats(1L)

        x.stats.keys.filter(y.stats.keys.toList.contains(_)).foreach(feature => feature match {
          case Feature.NUM_SAMP => newSample.set(feature,1)
          case Feature.NUM_REG     => newSample.set(feature,newNumRegions)
          case Feature.AVG_REG_LEN => newSample.set(feature,newAverageLen)
          case Feature.VARIANCE_LENGTH => newSample.set(feature,newVarianceLen)
          case Feature.MAX_COORD  => newSample.set(feature, Math.max(x.stats.get(feature).get, y.stats.get(feature).get))
          case Feature.MIN_COORD  => newSample.set(feature, Math.min(x.stats.get(feature).get, y.stats.get(feature).get))
          case Feature.MAX_LENGTH => newSample.set(feature, Math.max(x.stats.get(feature).get, y.stats.get(feature).get))
          case Feature.MIN_LENGTH => newSample.set(feature, Math.min(x.stats.get(feature).get, y.stats.get(feature).get))

          case _ => logger.warn("Feature "+feature.toString+" not supported for profile estimation.")
        })

        newSample
      })

      operator.outputProfile = Some(new GMQLDatasetProfile(List(mergedSamples)))

      //logger.info("Original left has "+dataset.outputProfile.get.samples.map(x=>x.get(Feature.NUM_REG)).reduce((a,b)=>a+b))
      //logger.info("Merged has: "+operator.outputProfile.get.samples.head.get(Feature.NUM_REG)+" regions.")
    }

    groupedDs
  }

  def assignGroups(dataset : RDD[GRECORD], grouping : RDD[FlinkMetaGroupType2]) : RDD[GRECORD] = {
    dataset.map(x => (x._1._1,x)).join(grouping).map{ x=> val r = x._2._1; val g = x._2._2
      (new GRecordKey(g, r._1._2, r._1._3, r._1._4, r._1._5), r._2)
    }
  }
}
