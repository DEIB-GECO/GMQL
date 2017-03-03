package it.polimi.genomics.spark.implementation.MetaOperators.GroupOperator

import java.nio.charset.StandardCharsets

import com.google.common.hash.Hashing
import it.polimi.genomics.core.DataStructures.MetaJoinCondition.{Default, Exact, FullName, MetaJoinCondition}
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
      val ref: RDD[MetaType] = executor.implement_md(leftDataset, sc).filter(v =>
        condition.attributes.foldLeft(false)((r,c) =>
          r |/* v._2._1.endsWith(c.toString())*/
            {
              if (c.isInstanceOf[FullName]) {
                (v._2._1.equals(c.asInstanceOf[FullName].attribute.toString()) || v._2._1.endsWith("." + c.asInstanceOf[FullName].attribute.toString()))
              }
              else if (c.isInstanceOf[Exact]) {
                v._2._1.equals(c.asInstanceOf[Exact].attribute.toString())
              }
              else {
                (v._2._1.equals(c.asInstanceOf[Default].attribute.toString()) || v._2._1.endsWith("." + c.asInstanceOf[Default].attribute.toString()))
              }
            }
        ))
        .map{x=>
          condition.attributes.flatMap{att=>
//            if(x._2._1.endsWith(att.toString()))Some((x._1,(att.toString(),x._2._2)))else None
            if (att.isInstanceOf[FullName]) {
              if (x._2._1.equals(att.asInstanceOf[FullName].attribute) || x._2._1.endsWith("." + att.asInstanceOf[FullName].attribute))
                Some((x._1, (x._2._1, x._2._2)))
              else None
            }
            else if (att.isInstanceOf[Exact]) {
              if (x._2._1.equals(att.asInstanceOf[Exact].attribute))
                Some((x._1, (att.asInstanceOf[Exact].attribute.toString(), x._2._2)))
              else None
            }
            else {
              if (x._2._1.equals(att.asInstanceOf[Default].attribute) || x._2._1.endsWith("." + att.asInstanceOf[Default].attribute))
                Some((x._1, (att.asInstanceOf[Default].attribute.toString(), x._2._2)))
              else None
            }
          }.head
        }
      val exp: RDD[MetaType] = executor.implement_md(rightDataset, sc).filter(v =>
        condition.attributes.foldLeft(false)((r,c) =>
          r | /*v._2._1.endsWith(c.toString())*/
            {
              if (c.isInstanceOf[FullName]) {
                (v._2._1.equals(c.asInstanceOf[FullName].attribute.toString()) || v._2._1.endsWith("." + c.asInstanceOf[FullName].attribute.toString()))
              }
              else if (c.isInstanceOf[Exact]) {
                v._2._1.equals(c.asInstanceOf[Exact].attribute.toString())
              }
              else {
                (v._2._1.equals(c.asInstanceOf[Default].attribute.toString()) || v._2._1.endsWith("." + c.asInstanceOf[Default].attribute.toString()))
              }
            }
        ))
        .map{x=>
          condition.attributes.flatMap{att=>
//            if(x._2._1.equals(att) || x._2._1.endsWith("."+att))Some((x._1,(att.toString,x._2._2)))else None
            if (att.isInstanceOf[FullName]) {
              if (x._2._1.equals(att.asInstanceOf[FullName].attribute) || x._2._1.endsWith("." + att.asInstanceOf[FullName].attribute))
                Some((x._1, (x._2._1, x._2._2)))
              else None
            }
            else if (att.isInstanceOf[Exact]) {
              if (x._2._1.equals(att.asInstanceOf[Exact].attribute))
                Some((x._1, (att.asInstanceOf[Exact].attribute.toString(), x._2._2)))
              else None
            }
            else {
              if (x._2._1.equals(att.asInstanceOf[Default].attribute) || x._2._1.endsWith("." + att.asInstanceOf[Default].attribute))
                Some((x._1, (att.asInstanceOf[Default].attribute.toString(), x._2._2)))
              else None
            }
          }.head
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

        var atts: Array[(String, Boolean)] = condition.attributes.map(x=>{
          if (x.isInstanceOf[FullName]) (x.asInstanceOf[FullName].attribute,false)
          else if (x.isInstanceOf[Exact]) (x.asInstanceOf[Exact].attribute,false)
          else (x.asInstanceOf[Default].attribute,false)
        }).toArray

        groupSampleByAtt.foreach(t=> atts.foreach(att=> if(t._1.endsWith(att._1)) atts.update(atts.indexOf(att),(att._1,true))))

        if(atts.filter(!_._2).size == 0)
//        if (groupSampleByAtt.size == condition.attributes.size)
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
            List(x1 + "§" + grid(0)._1 + x2)
          })
        }))
      }
    }

    splatterHelper(grid, List(""))
  }
}
