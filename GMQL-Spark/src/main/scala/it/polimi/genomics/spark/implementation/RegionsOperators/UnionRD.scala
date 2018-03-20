package it.polimi.genomics.spark.implementation.RegionsOperators

import com.google.common.hash.Hashing
import it.polimi.genomics.core.DataStructures.{GMQLDatasetProfile, GMQLSampleStats, IROperator, RegionOperator}
import it.polimi.genomics.core.DataTypes.GRECORD
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.core.{GNull, GRecordKey, GValue}
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

/**
  * Created by abdulrahman kaitoua on 13/07/15.
  */
object UnionRD {
  private final val logger = LoggerFactory.getLogger(this.getClass);

  @throws[SelectFormatException]
  def apply(operator: IROperator, executor: GMQLSparkExecutor, schemaReformatting: List[Int], leftDataset: RegionOperator, rightDataset: RegionOperator, sc: SparkContext) = {
    logger.info("----------------UnionRD executing..")

    //create the datasets
    val left =
      executor.implement_rd(rightDataset, sc)

    val right =
      executor.implement_rd(leftDataset, sc)

    def getNewId(pre: Long, id: Long): Long = {
      Hashing.md5.newHasher
        .putLong(pre).putLong(id)
        .hash
        .asLong
    }

    val leftMod: RDD[GRECORD] =
      left.map((r) => {
        val hash = getNewId(0L, r._1._1)
        (new GRecordKey(hash, r._1._2, r._1._3, r._1._4, r._1._5), r._2)
      })

    val rightMod: RDD[GRECORD] =
      right.map((r) => {
        val hash = getNewId(1L, r._1._1)
        (new GRecordKey(hash, r._1._2, r._1._3, r._1._4, r._1._5),
          schemaReformatting.foldLeft(new Array[GValue](0))((z, a) => {
            if (a.equals(-1)) {
              z :+ GNull()
            } else {
              z :+ r._2(a)
            }
          })
        )
      })

    //merge datasets
    val res = leftMod.union(rightMod)

    if( operator.requiresOutputProfile && rightDataset.outputProfile.isDefined && leftDataset.outputProfile.isDefined ) {

      // Merge profiles
      val leftSamples = leftDataset.outputProfile.get.samples.map( x => {
        val newId = getNewId(0L, x.ID)
        val newSampleProf = new GMQLSampleStats(newId)
        x.stats.foreach(stat => newSampleProf.set(stat._1, stat._2))
        newSampleProf
      })
      val rightSamples = rightDataset.outputProfile.get.samples.map( x => {
        val newId = getNewId(1L, x.ID)
        val newSampleProf = new GMQLSampleStats(newId)
        x.stats.foreach(stat => newSampleProf.set(stat._1, stat._2))
        newSampleProf
      })
      val newStats = leftSamples.union(rightSamples)
      operator.outputProfile = Some(new GMQLDatasetProfile(newStats))

      logger.info("Union profile has "+operator.outputProfile.get.samples.length+" samples")

      //logger.info("Original id" + left.take(1)(0)._1.id)
      //logger.info("Destination id" + leftSamples.head.ID)
      //logger.info("Destination id result" + res.take(1)(0)._1.id)

    }

    res

  }
}
