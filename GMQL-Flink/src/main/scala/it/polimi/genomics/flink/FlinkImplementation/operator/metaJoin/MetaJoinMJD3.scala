package it.polimi.genomics.flink.FlinkImplementation.operator.metaJoin

import it.polimi.genomics.core.DataStructures.MetaJoinCondition.MetaJoinCondition
import it.polimi.genomics.core.DataStructures.MetaOperator
import it.polimi.genomics.core.DataTypes.FlinkMetaJoinType
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.flink.FlinkImplementation.FlinkImplementation
import org.apache.flink.api.scala._
import org.apache.flink.hadoop.shaded.com.google.common.base.Charsets
import org.apache.flink.hadoop.shaded.com.google.common.hash.Hashing
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.collection.immutable.HashMap


/**
 * Created by michelebertoni on 13/05/15.
 */
object MetaJoinMJD3 {

  final val logger = LoggerFactory.getLogger(this.getClass)

  @throws[SelectFormatException]
  def apply(executor : FlinkImplementation, condition :  MetaJoinCondition, leftDataset : MetaOperator, rightDataset : MetaOperator, empty:Boolean, env : ExecutionEnvironment) : DataSet[FlinkMetaJoinType] = {

    val left = executor.implement_md(leftDataset, env)
    val right = executor.implement_md(rightDataset, env)

    if (!empty) {
      val dataset: DataSet[(Long, String, String, Boolean, Boolean)] =
        left.map((v) => (v._1, v._2/*.substring({
          val pos = v._2.lastIndexOf('.');
          if (pos < 0) 0 else pos
        })*/, v._3, true, false))
          .union(
            right.map((v) => (v._1, v._2/*.substring({
              val pos = v._2.lastIndexOf('.');
              if (pos < 0) 0 else pos
            })*/, v._3, false, true))
          )
          .filter((v) => condition.attributes.foldLeft(false)( _ | v._2.endsWith(_)))


      val sampleWithGroup: DataSet[FlinkMetaJoinType] =
        dataset
          .map((t) => {
            (t._1, HashMap((t._2, List(t._3))), t._4, t._5)
          })
          .groupBy(0, 2, 3)
          .reduce((a, b) => {
            (a._1, a._2.merged(b._2)({ case ((k, v1), (_, v2)) => (k, v1 ++ v2) }), a._3, a._4)
          })
          .filter((p) => p._2.size.equals(condition.attributes.size))
          .flatMap((p, out: Collector[FlinkMetaJoinType]) => {
            splatter(p._2.toList.sortBy(_._1)).map(v => {
              out.collect((p._1, Hashing.md5().hashString(v.toString, Charsets.UTF_8).asLong(), p._3, p._4))
            })

          })
      //(sampleID, groupID)

      sampleWithGroup
    }
    else
      {
        val value:Long = 1l

        left.map(x=>(x._1,value,true,false)).distinct()
          .union(right.map(x=>(x._1,value,false,true)).distinct())
      }
  }

  def splatter(grid : List[(String, List[String])]) : List[String] = {
    @tailrec
    def splatterHelper(grid : List[(String, List[String])], acc : List[String]) : List[String] = {
      grid.size match {
        case 0 => acc
        case _ => splatterHelper(grid.drop(1), grid(0)._2.flatMap((x2 : String) => { acc.flatMap((x1 : String) => { List(x1 + "§" + x2) }) }))
      }
    }

    splatterHelper(grid, List(""))
  }



}
