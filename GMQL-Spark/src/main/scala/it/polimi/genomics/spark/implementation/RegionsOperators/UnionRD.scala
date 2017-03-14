package it.polimi.genomics.spark.implementation.RegionsOperators

import com.google.common.hash.Hashing
import it.polimi.genomics.core.DataStructures.RegionOperator
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
  def apply(executor: GMQLSparkExecutor, schemaReformatting: List[Int], leftDataset: RegionOperator, rightDataset: RegionOperator, sc: SparkContext) = {
    logger.info("----------------UnionRD executing..")

    //create the datasets
    val left =
      executor.implement_rd(rightDataset, sc)

    val right =
      executor.implement_rd(leftDataset, sc)

    val leftMod: RDD[GRECORD] =
      left.map((r) => {
        val hash = Hashing.md5.newHasher
          .putLong(0L).putLong(r._1._1)
          .hash
          .asLong
        (new GRecordKey(hash, r._1._2, r._1._3, r._1._4, r._1._5), r._2)
      })

    val rightMod: RDD[GRECORD] =
      right.map((r) => {
        val hash =
          Hashing.md5.newHasher.putLong(1L).putLong(r._1._1)
            .hash
            .asLong
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
    leftMod.union(rightMod)
  }
}
