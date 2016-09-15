package it.polimi.genomics.flink.FlinkImplementation.operator.meta

import it.polimi.genomics.core.DataStructures.MetaOperator
import it.polimi.genomics.core.DataTypes.FlinkMetaType
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.flink.FlinkImplementation.FlinkImplementation
import org.apache.flink.api.scala._
import org.apache.flink.hadoop.shaded.com.google.common.hash.Hashing
import org.slf4j.LoggerFactory

/**
 * Created by michelebertoni on 06/05/15.
 */
object UnionMD {

  final val logger = LoggerFactory.getLogger(this.getClass)


  @throws[SelectFormatException]
  def apply(executor : FlinkImplementation, leftDataset : MetaOperator, rightDataset : MetaOperator, leftNameIn : String = "left", rightNameIn : String = "right", env : ExecutionEnvironment) = {

    //logger.warn("Executing UnionMD")

    //create the datasets
    val leftName = leftNameIn + "."
    val rightName = rightNameIn + "."

    val left = executor.implement_md(leftDataset, env).map((meta) => (meta._1, leftName + meta._2, meta._3))
    val right = executor.implement_md(rightDataset, env).map((meta) => (meta._1, rightName + meta._2, meta._3))

    //extract ids from datasets, compute the hash of the new id into a map
    val leftIds : Map[Long, Long] =
      left
        .map(_._1)
        .distinct()
        .collect
        .map((id) => (id, Hashing.md5().newHasher().putLong(0L).putLong(id).hash().asLong()))
        .toMap

    val rightIds : Map[Long, Long] =
      right
        .map(_._1).distinct()
        .collect
        .map((id) => (id, Hashing.md5().newHasher().putLong(1L).putLong(id).hash().asLong()))
        .toMap

    //change ID of each region according to previous computation
    val leftMod : DataSet[FlinkMetaType] =
      left.map((m) => {
        (leftIds.get(m._1).get, m._2, m._3)
      })

    val rightMod : DataSet[FlinkMetaType] =
      right.map((m) => {
        (rightIds.get(m._1).get, m._2, m._3)
      })

    //merge datasets
    leftMod.union(rightMod)
  }
}
