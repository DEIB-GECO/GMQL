package it.polimi.genomics.spark.implementation.RegionsOperators

import com.google.common.hash.Hashing
import it.polimi.genomics.core.DataStructures.{OptionalMetaJoinOperator, RegionOperator}
import it.polimi.genomics.core.DataTypes.GRECORD
import it.polimi.genomics.core.{GRecordKey, GValue}
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.collection.Map

/**
  * Created by abdulrahman kaitoua on 13/07/15.
  */
object GenometricDifference {

  private final val BINNING_PARAMETER = 50000
  private final val logger = LoggerFactory.getLogger(this.getClass);

  def apply(executor: GMQLSparkExecutor, grouping: OptionalMetaJoinOperator, leftDataset: RegionOperator, rightDataset: RegionOperator, exact:Boolean, sc: SparkContext): RDD[GRECORD] = {
    logger.info("----------------Differnce executing..")

    //creating the datasets
    val ref: RDD[GRECORD] =
      executor.implement_rd(leftDataset, sc)
    val exp: RDD[GRECORD] =
      executor.implement_rd(rightDataset, sc)

    val groups: Map[Long, Array[Long]] =
      executor.implement_mjd(grouping, sc).collectAsMap()

    val Bgroups: Broadcast[Map[Long, Array[Long]]] = sc.broadcast(groups)
    //group the datasets
    val groupedRef: RDD[((Long, String, Int), (Long, Long, Long, Char, Array[GValue]))] =
      binRefDS(ref, BINNING_PARAMETER, Bgroups).distinct
    val groupedExp: RDD[((Long, String, Int), (Long, Long, Char, Array[GValue]))] =
      binExpDS(exp, BINNING_PARAMETER)


    //Join phase
    val coGroupResult: RDD[(Long, (Long, String, Long, Long, Char, Array[GValue], Int))] =
      groupedRef // groupId, bin, chromosome
        .cogroup(groupedExp).flatMap { x =>
        val ref = x._2._1;
        val exp = x._2._2;
        val key = x._1
        ref.map { region =>
          var count = 0
          val newID = Hashing.md5().newHasher().putLong(region._1).hash().asLong()
          val aggregation = Hashing.md5().newHasher().putString(newID + key._2 + region._2 + region._3 + region._4 + region._5.mkString("/"), java.nio.charset.Charset.defaultCharset()).hash().asLong()
          for (experiment <- exp)
            if ( /* cross */
            /* space overlapping */
              (region._2 < experiment._2 && experiment._1 < region._3)
                && /* same strand */
                (region._4.equals('*') || experiment._3.equals('*') || region._4.equals(experiment._3))
                && /* first comparison */
                ((region._2 / BINNING_PARAMETER).toInt.equals(key._3) || (experiment._1 / BINNING_PARAMETER).toInt.equals(key._3))
            ) {
              if(!exact || (region._2 == experiment._1 && region._3 == experiment._2))
                count = count + 1;
            }

          (aggregation, (newID, key._2, region._2, region._3, region._4, region._5, count))
        }
      }


    val filteredReduceResult: RDD[(GRecordKey, Array[GValue])] =
    // group regions by aggId (same region)
      coGroupResult
        // reduce phase -> sum the count value of left and right
        .reduceByKey((r1, r2) =>
        (r1._1, r1._2, r1._3, r1._4, r1._5, r1._6, r1._7 + r2._7)
      )
        // filter only region with count = 0
        .flatMap(r =>
        if (r._2._7.equals(0))
          Some((new GRecordKey(r._2._1, r._2._2, r._2._3, r._2._4, r._2._5), r._2._6))
        else
          None
      )

    //OUTPUT
    filteredReduceResult

  }

  def binRefDS(rdd: RDD[GRECORD], bin: Long, Bgroups: Broadcast[Map[Long, Array[Long]]]): RDD[((Long, String, Int), (Long, Long, Long, Char, Array[GValue]))] =
    rdd.flatMap { x =>
      val startbin = (x._1._3 / bin).toInt
      val stopbin = (x._1._4 / bin).toInt
      val group: Option[Array[Long]] = Bgroups.value.get(x._1._1)
      if (group.isDefined) {
        (startbin to stopbin).flatMap(i =>
          group.get.map(id => (((id, x._1._2, i), (x._1._1, x._1._3, x._1._4, x._1._5, x._2))))
        )
      } else
        None
    }

  def binExpDS(rdd: RDD[GRECORD], bin: Long): RDD[((Long, String, Int), (Long, Long, Char, Array[GValue]))] =
    rdd.flatMap { x =>
      val startbin = (x._1._3 / bin).toInt
      val stopbin = (x._1._4 / bin).toInt
      for (i <- startbin to stopbin)
        yield ((x._1._1, x._1._2, i), (x._1._3, x._1._4, x._1._5, x._2))
    }


}
