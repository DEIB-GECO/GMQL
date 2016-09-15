package LowLevel.FlinkImplementation.metaJoinOperator

import it.polimi.genomics.core.DataStructures.MetaJoinCondition.MetaJoinCondition
import it.polimi.genomics.core.DataStructures.MetaOperator
import it.polimi.genomics.core.DataTypes.{FlinkMetaJoinType3, FlinkMetaType}
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.flink.FlinkImplementation.FlinkImplementation
import org.apache.flink.api.scala._
import org.apache.flink.hadoop.shaded.com.google.common.base.Charsets
import org.apache.flink.hadoop.shaded.com.google.common.hash.Hashing
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.collection.mutable


/**
 * Created by michelebertoni on 13/05/15.
 */
object MetaJoinMJD {

  final val logger = LoggerFactory.getLogger(this.getClass)


  //val hf : HashFunction = Hashing.sha256()

  @throws[SelectFormatException]
  def apply(executor : FlinkImplementation, condition :  MetaJoinCondition, leftDataset : MetaOperator, rightDataset : MetaOperator, env : ExecutionEnvironment) : DataSet[FlinkMetaJoinType3] = {


    val dataset : DataSet[(Long, String, String)] =
      executor.implement_md(leftDataset, env)
        .union(
          executor.implement_md(rightDataset, env)
        )
        .filter((v : FlinkMetaType) => condition.attributes.contains(v._2))


    val sampleWithGroup : DataSet[(Long, Long)] =
      dataset
        .filter((m : FlinkMetaType) => condition.attributes.contains(m._2))
        .map((t) => {
          (t._1, mutable.HashMap((t._2, List(t._3))))
        })
        .groupBy(_._1)
        .reduce((a,b) => {
          b._2.foreach((v) => {
            a._2.get(v._1) match {
              case None => a._2.put(v._1, v._2)
              case Some(l) => a._2(v._1) = l ++ v._2
            }
          })
          a
        })
        .filter((p) => p._2.size.equals(condition.attributes.size))
        .flatMap((p, out: Collector[(Long, Long)]) =>{
          splatter(p._2.toList.sortBy(_._1)).map(v => {
            out.collect((p._1, Hashing.md5().hashString(v.toString, Charsets.UTF_8).asLong()))
          })
        })
    //(sampleID, groupID)

    val sampleSamplePairs =
      sampleWithGroup
        .join(sampleWithGroup).where(1).equalTo(1){
          (a, b, out: Collector[(Long, Long)]) =>
            if(!a._1.equals(b._1)) {
              out.collect((a._1, b._1))
            }
        }
        .distinct
    //(sampleID, sampleID)



    //logger.error("--------------------- METAJOINMJD pairs : " + sampleSamplePairs)
    sampleSamplePairs

  }

  def splatter(grid : List[(String, List[String])]) : List[String] = {
    @tailrec
    def splatterHelper(grid : List[(String, List[String])], acc : List[String]) : List[String] = {
      val sb = new StringBuilder
      grid.size match{
        //returns a list for compatibility with previous splatter (see below) that allowed different grouping methods
        case 0 => List(acc.tail.mkString("#"))
        case _ => splatterHelper(
          grid.drop(1),
          acc :+ grid(0)._2.mkString("§")
        )
      }
    }
    splatterHelper(grid, List[String](""))
  }

  /*
  def splatter(grid : List[(String, List[String])]) : List[String] = {
    @tailrec
    def splatterHelper(grid : List[(String, List[String])], acc : List[String]) : List[String] = {
      val sb = new StringBuilder
      grid.size match{
        case 0 => acc
        case _ => splatterHelper(
          grid.drop(1),
          power(grid(0)._2)
            .flatMap((s) => {
            acc.map((v) => {
              //v+"#"+s
              sb.setLength(0)
              sb.append(v)
              sb.append("#")
              sb.append(s)
              sb.toString()
            })
          }))
      }
    }
    splatterHelper(grid, List[String](""))
  }


  def power(l : List[String]) : List[String] = {
    @tailrec
    def powerHelper(l : List[String], acc : List[List[String]]) : List[String] = {
      l.size match{
        case 0 => acc.map((line) => line.mkString("§"))
        case _ => powerHelper(l.drop(1), acc ++ acc.map((v : List[String]) => v :+ l.head))
      }
    }

    val sortedList = l.sorted
    powerHelper(sortedList, List(List[String]())).drop(1) //must remove the empty element
  }
  */

  /*
  def splatter(grid : List[(String, List[String])]) : List[String] = {
    grid.size match {
      case 1 => grid(0)._2
      case _ => distributor(grid(0)._2, splatter(grid.drop(0)))
    }
  }

  def distributor(l1 : List[String], l2 : List[String]) : List[String] = {
    val sb = new StringBuilder
    //l1.flatMap(s => l2.map(_ + s))
    val l1ser =
      l1.mkString("§")

    l2.map(_ + l1ser)

  }
  */


}
