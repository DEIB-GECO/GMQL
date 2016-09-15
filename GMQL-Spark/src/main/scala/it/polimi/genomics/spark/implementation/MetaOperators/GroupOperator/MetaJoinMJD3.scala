package it.polimi.genomics.spark.implementation.MetaOperators.GroupOperator

import com.google.common.base.Charsets
import com.google.common.hash.Hashing
import it.polimi.genomics.core.DataStructures.MetaJoinCondition.MetaJoinCondition
import it.polimi.genomics.core.DataStructures.MetaOperator
import it.polimi.genomics.core.DataTypes.FlinkMetaJoinType3
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.annotation.tailrec
import scala.collection.immutable.HashMap


/**
 * Created by Abdulrahman kaitoua on 13/07/15.
 */
object MetaJoinMJD3 {

  //final val logger = LoggerFactory.getLogger(this.getClass)

  @throws[SelectFormatException]
  def apply(executor : GMQLSparkExecutor, condition :  MetaJoinCondition, leftDataset : MetaOperator, rightDataset : MetaOperator, sc : SparkContext) : RDD[FlinkMetaJoinType3] = {


    val dataset : RDD[(Long,( String, String))] =
      executor.implement_md(leftDataset, sc)
        .union(
          executor.implement_md(rightDataset, sc)
        )
        .filter(v => condition.attributes.contains(v._2._1))


    val sampleWithGroup : RDD[(Long, Long)] =
      dataset
        .map{t =>(t._1, HashMap((t._2._1, List(t._2._2))))}
        .reduceByKey{(a,b) =>a.merged(b)({case ((k,v1),(_,v2)) => (k,v1++v2)})}
        .filter((p) => p._2.size.equals(condition.attributes.size))
        .flatMap { p =>
        splatter(p._2.toList.sortBy(_._1)).map(v=>(p._1,Hashing.md5.newHasher.putString(v, Charsets.UTF_8).hash.asLong))
      }
//    sampleWithGroup.collect().foreach(x=>println("sample/group: ",x))
    //(sampleID, groupID)
    sampleWithGroup
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
