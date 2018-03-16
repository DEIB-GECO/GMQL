package it.polimi.genomics.spark.implementation.MetaOperators

import com.google.common.hash.Hashing
import it.polimi.genomics.core.DataStructures.JoinParametersRD.RegionBuilder
import it.polimi.genomics.core.DataStructures.JoinParametersRD.RegionBuilder.RegionBuilder
import it.polimi.genomics.core.DataStructures.{MetaOperator, OptionalMetaJoinOperator, SomeMetaJoinOperator}
import it.polimi.genomics.core.DataTypes.MetaType
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * Created by Abdulrahman Kaitoua on 17/06/15.
  */
object CombineMD {

  //val hf : HashFunction = Hashing.sha256()

  //final val logger = LoggerFactory.getLogger(this.getClass)
  private final val logger = LoggerFactory.getLogger(CombineMD.getClass);

  @throws[SelectFormatException]
  def apply(executor: GMQLSparkExecutor, grouping: OptionalMetaJoinOperator,
            leftDataset: MetaOperator, rightDataset: MetaOperator,
            region_builder: Option[RegionBuilder],

            leftTag: String = "left", rightTag: String = "right",
            sc: SparkContext): RDD[MetaType] = {

    logger.info("----------------CombineMD executing..")

    val distinct =
      if (region_builder.isDefined)
        region_builder.get match {
          case RegionBuilder.LEFT_DISTINCT => true
          case RegionBuilder.RIGHT_DISTINCT => true
          case _ => false
        } else false

    val left = executor.implement_md(leftDataset, sc).repartition(32)
    val right = executor.implement_md(rightDataset, sc).repartition(32)

    val ltag =
      if (!leftTag.isEmpty() && !distinct) {
        leftTag + "."
      }
      else ""
    val rtag =
      if (!rightTag.isEmpty() && !distinct) {
        rightTag + "."
      }
      else ""

    if (grouping.isInstanceOf[SomeMetaJoinOperator]) {
      val pairs: Broadcast[collection.Map[Long, Array[Long]]] = sc.broadcast(executor.implement_mjd(grouping, sc).collectAsMap())


      val mapL = collection.mutable.HashMap[Long, mutable.Set[Long]]()
      val mapR = collection.mutable.HashMap[Long, mutable.Set[Long]]()

      pairs.value.foreach { case (left, rights) =>
        rights.foreach { right =>
          val hash = Hashing.md5().newHasher().putLong(left).putLong(right).hash().asLong
          //in each cycle, we added new hash id in to the left and right lists
          mapL += left -> (mapL.getOrElse(left, mutable.Set.empty) + hash)
          mapR += right -> (mapR.getOrElse(right, mutable.Set.empty) + hash)
        }
      }

      val leftOut = left
        .filter { case (id: Long, _) => mapL.contains(id) }
        .flatMap { case (leftId: Long, (att: String, value: String)) =>
          val taggedAtt = ltag + att
          mapL(leftId).map { newId =>
            (newId, (taggedAtt, value))
          }
        }

      val rightOut = right
        .filter { case (id: Long, _) => mapR.contains(id) }
        .flatMap { case (rightId: Long, (att: String, value: String)) =>
          val taggedAtt = rtag + att
          mapR(rightId).map { newId =>
            (newId, (taggedAtt, value))
          }
        }

      if (region_builder.isDefined)
        region_builder.get match {
          case RegionBuilder.LEFT_DISTINCT => leftOut
          case RegionBuilder.RIGHT_DISTINCT => rightOut
          case _ => leftOut.union(rightOut)
        }
      else
        leftOut.union(rightOut)

    } else { //not grouping.isInstanceOf[SomeMetaJoinOperator]
      val leftIds = sc.broadcast(left.keys.distinct().collect()).value
      val rightIds = sc.broadcast(right.keys.distinct().collect()).value

      val mapL = collection.mutable.HashMap[Long, mutable.Set[Long]]()
      val mapR = collection.mutable.HashMap[Long, mutable.Set[Long]]()

      leftIds.foreach { left =>
        rightIds.foreach { right =>
          val hash = Hashing.md5().newHasher().putLong(left).putLong(right).hash().asLong
          //in each cycle, we added new hash id in to the left and right lists
          mapL += left -> (mapL.getOrElse(left, mutable.Set.empty) + hash)
          mapR += right -> (mapR.getOrElse(right, mutable.Set.empty) + hash)
        }
      }

      val leftOut = left
        .filter { case (id: Long, _) => mapL.contains(id) }
        .flatMap { case (leftId: Long, (att: String, value: String)) =>
          val taggedAtt = ltag + att
          mapL(leftId).map { newId =>
            (newId, (taggedAtt, value))
          }
        }

      val rightOut = right
        .filter { case (id: Long, _) => mapR.contains(id) }
        .flatMap { case (rightId: Long, (att: String, value: String)) =>
          val taggedAtt = rtag + att
          mapR(rightId).map { newId =>
            (newId, (taggedAtt, value))
          }
        }


      if (region_builder.isDefined)
        region_builder.get match {
          case RegionBuilder.LEFT_DISTINCT => leftOut
          case RegionBuilder.RIGHT_DISTINCT => rightOut
          case _ => leftOut.union(rightOut)
        }
      else
        leftOut.union(rightOut) //.sortBy(x=>x._1)
    }
  }
}
