package it.polimi.genomics.spark.implementation.MetaOperators.GroupOperator

import java.nio.charset.StandardCharsets

import com.google.common.hash.Hashing
import it.polimi.genomics.core.DataStructures.MetaJoinCondition.MetaJoinCondition
import it.polimi.genomics.core.DataStructures.MetaOperator
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.annotation.tailrec


/**
  * Created by abdulrahman Kaitoua on 13/07/15.
  */
object MetaJoinMJD2 {

  private final val logger = LoggerFactory.getLogger(MetaJoinMJD2.getClass);

  @throws[SelectFormatException]
  def apply(executor: GMQLSparkExecutor, condition: MetaJoinCondition, leftDataset: MetaOperator, rightDataset: MetaOperator, empty: Boolean, sc: SparkContext): RDD[SparkMetaJoinType] = {
    logger.info("----------------MetaJoinMD2 executing..")
    if (!empty) {
      println("condition atributes: ",condition.attributes)
      val ref: RDD[MetaType] = executor.implement_md(leftDataset, sc).filter(v =>  condition.attributes.foldLeft(false)( _ | v._2._1.endsWith(_)))
        .map{x=>
          condition.attributes.flatMap{att=> if(x._2._1.equals(att) || x._2._1.endsWith("."+att))Some((x._1,(att,x._2._2)))else None}.head
        }
      val exp: RDD[MetaType] = executor.implement_md(rightDataset, sc).filter(v =>  condition.attributes.foldLeft(false)( _ | v._2._1.endsWith(_)))
        .map{x=>
          condition.attributes.flatMap{att=> if(x._2._1.equals(att) || x._2._1.endsWith("."+att))Some((x._1,(att,x._2._2)))else None}.head
        }

      //ref, Array[exp]
      val sampleWithGroup: RDD[(Long, Array[Long])] =
        MJexecutor(ref, condition).join(MJexecutor(exp, condition)).map(x => x._2).distinct.groupByKey().mapValues(_.toArray)
      sampleWithGroup
    } else {
      val right = executor.implement_md(rightDataset, sc).keys.distinct.collect
      val left = executor.implement_md(leftDataset, sc).keys.distinct()
      left.map(x => (x, right))
    }

  }

  //(GID,id)
  def MJexecutor(ds: RDD[MetaType], condition: MetaJoinCondition): RDD[(Long, Long)] = {
    ds.groupByKey() //.filter(p => p._2.size.equals(condition.attributes.size))
      .flatMap { x =>
      //      println ("x ",x)
      val itr = x._2
      if (!itr.iterator.hasNext) None
      else
      //      itr.map{att=>(Hashing.md5.newHasher().putString(att._1+att._2,StandardCharsets.UTF_8).hash().asLong,x._1)}
      {
        val groupSampleByAtt = itr.groupBy(_._1)
        if (groupSampleByAtt.size == condition.attributes.size)
          splatter(groupSampleByAtt.map(x => (x._1, x._2.map(_._2).toList)).toList).
            map(groupString => (Hashing.md5.newHasher().putString(groupString, StandardCharsets.UTF_8).hash().asLong, x._1))
        else
          None
      }
    }
  }

  def splatter(grid: List[(String, List[String])]): List[String] = {
    @tailrec
    def splatterHelper(grid: List[(String, List[String])], acc: List[String]): List[String] = {
      grid.size match {
        case 0 => acc
        case _ => splatterHelper(grid.drop(1), grid(0)._2.flatMap((x2: String) => {
          acc.flatMap((x1: String) => {
            List(x1 + "§" + x2)
          })
        }))
      }
    }

    splatterHelper(grid, List(""))
  }
}
