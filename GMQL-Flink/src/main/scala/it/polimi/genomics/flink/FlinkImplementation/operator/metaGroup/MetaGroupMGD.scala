package it.polimi.genomics.flink.FlinkImplementation.operator.metaGroup

import it.polimi.genomics.core.DataStructures.MetaGroupByCondition.MetaGroupByCondition
import it.polimi.genomics.core.DataStructures.MetaOperator
import it.polimi.genomics.core.DataTypes.{FlinkMetaGroupType2, FlinkMetaType}
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
 * Created by michelebertoni on 24/05/15.
 */
object MetaGroupMGD {
  final val logger = LoggerFactory.getLogger(this.getClass)

  @throws[SelectFormatException]
  def apply(executor : FlinkImplementation, condition : MetaGroupByCondition, inputDataset : MetaOperator, env : ExecutionEnvironment) : DataSet[FlinkMetaGroupType2] = {

    val dataset = executor.implement_md(inputDataset, env)
    execute(condition, dataset)
  }


  def execute(condition : MetaGroupByCondition, dataset : DataSet[(Long, String, String)]) : DataSet[FlinkMetaGroupType2] = {

    val sampleWithOneGroup : DataSet[FlinkMetaGroupType2] =
      dataset
        .flatMap((m : FlinkMetaType, out : Collector[FlinkMetaType]) => {
        val key = m._2.substring({val pos = m._2.lastIndexOf('.'); if(pos < 0) 0 else pos})
          if(condition.attributes.contains(key)) {
            out.collect((m._1, key, m._3))
          }
        })
        .map((t) => {
          (t._1, HashMap((t._2, List(t._3))))
        })
        .groupBy(_._1)
        .reduce((a,b) => {
          (a._1, a._2.merged(b._2)({case ((k,v1),(_,v2)) => (k,v1++v2)}))
        })
        /*.reduce((a,b) => {
          b._2.foreach((v) => {
            a._2.get(v._1) match {
              case None => a._2.put(v._1, v._2)
              case Some(l) => a._2(v._1) = l ++ v._2
            }
          })
          a
        })
        */
        //.filter((p) => p._2.size.equals(condition.attributes.size))
        .map((p/*, out: Collector[FlinkMetaGroupType2]*/) =>{
          /*
          splatter(p._2.toList.sortBy(_._1)).map(v => {
            out.collect((p._1, List(Hashing.md5().hashString(v.toString, Charsets.UTF_8).asLong())))
          })
          */
          /*out.collect((p._1, Hashing.md5().hashString(splatter(p._2.toList.sortBy(_._1)), Charsets.UTF_8).asLong()))*/
          //out.collect(
            (p._1, Hashing.md5().hashString(p._2.map((x) => (x._1, x._2.sorted.mkString("§"))).toList.sortBy(_._1).mkString("£"), Charsets.UTF_8).asLong())
          //)
        })
    //(sampleID, groupID)

    /*
    val sampleIdWithGroups : DataSet[(Long, List[Long])] =
      sampleWithOneGroup
        .groupBy(0)
        .reduce((a : (Long, List[Long]), b : (Long, List[Long])) =>
          (a._1, a._2 ++ b._2)
        )

    //(sampleId, List[groupIDs])
    */
    sampleWithOneGroup
  }


  def splatter(grid : List[(String, List[String])]) : String = {
    @tailrec
    def splatterHelper(grid : List[(String, List[String])], acc : List[String]) : String = {
      val sb = new StringBuilder
      grid.size match{
        //returns a list for compatibility with previous splatter (see below) that allowed different grouping methods
        case 0 => acc.tail.mkString("£")
        case _ => splatterHelper(
          grid.drop(1),
          acc :+ grid(0)._2.sorted.mkString("§")
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
    //l1.flatMap(s => l2.map(_ + s))
    power(l1).flatMap(s => l2.map(_ + s))
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


}
