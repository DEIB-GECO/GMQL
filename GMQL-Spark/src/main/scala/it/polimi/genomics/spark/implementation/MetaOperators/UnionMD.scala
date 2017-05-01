package it.polimi.genomics.spark.implementation.MetaOperators

import com.google.common.hash.Hashing
import it.polimi.genomics.core.DataStructures.MetaOperator
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

/**
 * Created by abdulrahman kaitoua on 04/07/15.
 */
object UnionMD {

  private final val logger = LoggerFactory.getLogger(UnionMD.getClass);

  @throws[SelectFormatException]
  def apply(executor : GMQLSparkExecutor,  rightDataset : MetaOperator,leftDataset : MetaOperator,leftTag: String = "left", rightTag :String = "right", sc : SparkContext) = {

    logger.info("----------------UnionMD executing..")

    //create the datasets
    val left: RDD[(Long, (String, String))] =
      executor.implement_md(leftDataset, sc)

    val right: RDD[(Long, (String, String))] =
      executor.implement_md(rightDataset, sc)

//    logger.info("sizes (lef,right):"+left.count+","+right.count+":"+leftTag+","+rightTag)
    //change ID of each region according to previous computation
    val leftMod  =
      left.map((m) => {
//        if (!leftTag.isEmpty())
//          (Hashing.md5.newHasher.putLong(0L).putLong(m._1).hash.asLong, (leftTag + "." + m._2._1, m._2._2))
//        else
          (Hashing.md5.newHasher.putLong(0L).putLong(m._1).hash.asLong, (m._2._1, m._2._2))
      })

    val rightMod  =
      right.map((m) => {
//        if (!rightTag.isEmpty())
//          (Hashing.md5.newHasher.putLong(1L).putLong( m._1).hash.asLong, (rightTag+"."+m._2._1,m._2._2))
//        else
          (Hashing.md5.newHasher.putLong(1L).putLong( m._1).hash.asLong, (m._2._1,m._2._2))
      })

    //merge datasets
    //(leftMod.union(rightMod)).distinct()
    //TODO fast fix, get rid of hard-coded attribute name
    val newAtt: RDD[(Long, (String, String))] = leftMod.distinct().groupByKey().map{ x => (x._1, ("_provenance", leftTag))}.union(rightMod.distinct().groupByKey().map{ x => (x._1, ("_provenance", rightTag))})
    (leftMod.union(rightMod)).distinct().union(newAtt)

  }
}
