package it.polimi.genomics.flink.FlinkImplementation.operator.region

import it.polimi.genomics.core.DataStructures.RegionOperator
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.{GNull, GValue}
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.flink.FlinkImplementation.FlinkImplementation
import org.apache.flink.api.scala._
import org.apache.flink.hadoop.shaded.com.google.common.hash.Hashing
import org.slf4j.LoggerFactory

/**
 * Created by michelebertoni on 04/06/15.
 */
object UnionRD {

  final val logger = LoggerFactory.getLogger(this.getClass)
  @throws[SelectFormatException]
  def apply(executor : FlinkImplementation, schemaReformatting : List[Int], leftDataset : RegionOperator, rightDataset : RegionOperator, env : ExecutionEnvironment) = {
    //create the datasets
    val left : DataSet[FlinkRegionType] =
      executor.implement_rd(rightDataset, env)

    val right : DataSet[FlinkRegionType] =
      executor.implement_rd(leftDataset, env)

    //extract ids from datasets, compute the hash of the new id into a map
    val leftIds : Map[Long, Long] =
      left
        .map(_._1)
        .distinct()
        .collect
        .map((id) =>{ (id, Hashing.md5().newHasher().putLong(0L).putLong(id).hash().asLong())})
        //.map((id) => (id, Hashing.md5().hashString("0" + id, Charsets.UTF_8).asLong()))
        .toMap

    val rightIds : Map[Long, Long] =
      right
        .map(_._1).distinct()
        .collect
        .map((id) => { (id, Hashing.md5().newHasher().putLong(1L).putLong(id).hash().asLong())})
        //.map((id) => (id, Hashing.md5().hashString("1" + id, Charsets.UTF_8).asLong()))
        .toMap

    //change ID of each region according to previous computation
    val leftMod : DataSet[FlinkRegionType] =
      left.map((r) => {
        (leftIds.get(r._1).get, r._2, r._3, r._4, r._5, r._6)
      })


    val rightMod : DataSet[FlinkRegionType] =
      right.map((r) => {
        (rightIds.get(r._1).get, r._2, r._3, r._4, r._5,
          schemaReformatting.foldLeft(Array[GValue]())((z, a) => {
            if(a.equals(-1)){
              z :+ GNull()
            } else {
              z :+ r._6(a)
            }
          })
        )
      })

    //merge datasets
    leftMod.union(rightMod)
  }
}
