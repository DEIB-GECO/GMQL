package it.polimi.genomics.spark.implementation.MetaOperators

import com.google.common.base.Charsets
import com.google.common.hash.Hashing
import it.polimi.genomics.core.DataStructures.MetaOperator
import it.polimi.genomics.core.Debug.EPDAG
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

/**
 * Created by abdulrahman kaitoua on 04/07/15.
 */
object UnionAggMD {

  private final val logger = LoggerFactory.getLogger(UnionMD.getClass);

  @throws[SelectFormatException]
  def apply(executor : GMQLSparkExecutor, leftDataset : MetaOperator, rightDataset : MetaOperator,leftTag: String = "left", rightTag :String = "right", sc : SparkContext) = {

    logger.info("----------------UnionMD executing..")

    //create the datasets
    val left: RDD[(Long, (String, String))] =
      executor.implement_md(leftDataset, sc)._2

    val right: RDD[(Long, (String, String))] =
      executor.implement_md(rightDataset, sc)._2

    val startTime: Float = EPDAG.getCurrentTime

    //change ID of each region according to previous computation
    val leftMod  =
      left.map((m) => {
        (/*Hashing.md5.newHasher.putLong(1L).putLong(*/m._1/*).hash.asLong*/, (/*leftTag +"."+*/m._2._1,m._2._2))
      })

    val rightMod  =
      right.map((m) => {
        (/*Hashing.md5.newHasher.putLong(2L).putLong(*/ m._1/*).hash.asLong*/, (/*rightTag+"."+*/m._2._1,m._2._2))
      })
    //merge datasets
    (startTime, (leftMod.union(rightMod)).distinct())
  }


}
