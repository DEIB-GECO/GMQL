package it.polimi.genomics.spark.implementation.RegionsOperators

import java.io

import com.google.common.base.Charsets
import com.google.common.hash.Hashing
import it.polimi.genomics.core.DataStructures.GroupMDParameters.Direction.Direction
import it.polimi.genomics.core.DataStructures.GroupMDParameters._
import it.polimi.genomics.core.DataStructures.RegionOperator
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.core.{GDouble, GNull, GRecordKey, GValue}
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.{Partitioner, SparkContext}
import org.slf4j.LoggerFactory

import scala.collection.immutable.HashMap

/**
 * Created by abdulrahman kaitoua on 13/07/15.
 */
object OrderRD {
  private final val logger = LoggerFactory.getLogger(this.getClass);

  @throws[SelectFormatException]
  def apply(executor : GMQLSparkExecutor, ordering : List[(Int, Direction)], topParameter : TopParameter, inputDataset : RegionOperator, sc : SparkContext) : RDD[GRECORD] = {
    logger.info("----------------OrderRD executing..")

    val ds:RDD[GRECORD] =
      executor.implement_rd(inputDataset, sc)


    val grouping : Boolean =
      topParameter match {
        case NoTop() => false
        case Top(_) => false
        case TopP(_) => true
        case TopG(_) => true
      }

    val top : Int =
      topParameter match {
        case NoTop() => 0
        case Top(v) => v
        case TopP(v) => v
        case TopG(v) => v
      }

    var index = 0
    val valuesOrdering: Ordering[Array[GValue]] =  orderByColumn(0) orElse ordering.map(x=>  x._2 match {
      case Direction.ASC => println(x);index = index+1;orderByColumn(index)
      case Direction.DESC => println(x);index = index+1;orderByColumn(index).reverse
    } ).reduceLeft((res,x)=>res orElse x)

    //extract the indexes that will be used in grouping/ordering
    val keys : List[Int] =
      ordering.map(_._1)

    val sortedGroupsOfRegions : RDD[GRECORD] =
      if(grouping){
        sort( ds.groupBy{r =>
          val s = new StringBuilder
          s.append(r._1._1)
          keys.init.foreach((i) => s.append(r._2(i)))
          Hashing.md5.newHasher.putString(s.toString(), Charsets.UTF_8).hash.asLong
        },valuesOrdering,ordering,top,topParameter)
      } else {
        val ids = ds.map(_._1._1).distinct().collect
        val dss: RDD[(Long, (GRecordKey, Array[GValue]))] = ds.keyBy(x=>x._1._1)
        val partitionedData: RDD[(Long,GRECORD)] = {
//          val part = new RangePartitioner(dss.partitions.size, dss, true)
          val part = new IDPartitioner(ids,ids.size)
          new ShuffledRDD[Long, (GRecordKey, Array[GValue]), (GRecordKey, Array[GValue])](dss, part)
            //.setKeyOrdering(Ordering.Long)
        }

        var buffer: GRECORD = null /*partitionedData.first()._2*/
        partitionedData.mapPartitions{part=>
          var position = 0
          val ddeded = part.toArray

          val ddNull = ddeded.flatMap{ x => val p = ordering.map(o => x._2._2(o._1)); if (p.head.isInstanceOf[GNull]) Some(x) else None}
          val ddDouble = ddeded.flatMap{x => val p = ordering.map(o => x._2._2(o._1)); if (p.head.isInstanceOf[GDouble]) Some(x) else None}
          buffer = /*ddeded*/ddDouble(0)._2
          val ddd= /*ddeded*/ddDouble.sortBy{e=>GDouble(e._1.toDouble).asInstanceOf[GValue] +:  ordering.map(o=>e._2._2(o._1)).toArray}(valuesOrdering).union(ddNull)
          //x =>  ordering.map(o => x._2(o._1)).toArray
          ddd.flatMap { record =>

            if(buffer._1._1 != record._2._1._1) {
                            buffer = record._2;
                            position=0
                          }
            position = position +1;
            if((top>0 && position<=top)|| top ==0)
                          Some(record._2._1, record._2._2 :+ GDouble(position))
            else
              None
          }.iterator
        }
      }
    sortedGroupsOfRegions
  }

  def sort(ds:RDD[(Long,Iterable[GRECORD])],valuesOrdering: Ordering[Array[GValue]],ordering: List[(Int, Direction)],top:Int, topParameter : TopParameter): RDD[GRECORD] ={
    val data = ds.mapValues(it => it.toList.sortBy( x =>  GDouble(x._1._1.toDouble).asInstanceOf[GValue] +: ordering.map(o => x._2(o._1)).toArray)(/*ordering.head._2 match {
      case Direction.ASC => Ordering[Iterable[GValue]];
      case Direction.DESC =>Ordering[Iterable[GValue]].reverse}*/
        valuesOrdering
      )
    )
    val percentages= if(topParameter.isInstanceOf[TopP]){
       data.map(x=>(x._1,x._2.size * top/100)).collectAsMap()
    }else HashMap[Long,Int]()


    data
      .flatMap{x=>
        val itr = if(top>0) x._2.take(percentages.get(x._1).getOrElse(top))
        else x._2;

        var position = 0
      itr.map{c=>
        position = position +1;
        (c._1, c._2 :+ GDouble(position))
      }
    }
  }


  // Functions and objects needed for sorting by multi values.
  def orderByColumn(col: Int) = Ordering.by { ar: Array[GValue] => ar(col) }

  final class CompositeOrdering[T]( val ord1: Ordering[T], val ord2: Ordering[T] ) extends Ordering[T] {
    def compare( x: T, y: T ) = {
      val comp = ord1.compare( x, y )
      if ( comp != 0 ) comp else ord2.compare( x, y )
    }
  }

  object CompositeOrdering {
    def apply[T]( orderings: Ordering[T] * ) = orderings reduceLeft (_ orElse _)
  }

  implicit class OrderingOps[T]( val ord: Ordering[T] ) extends AnyVal {
    def orElse( ord2: Ordering[T] ) = new CompositeOrdering[T]( ord, ord2 )
  }

  //partitioners
  class IDPartitioner(ids:Array[Long],partitions: Int) extends Partitioner {
    def numPartitions: Int = partitions

    def getPartition(key: Any): Int = key match {
      case null => 0
      case _ => ids.indexOf(key.asInstanceOf[Long])
//      case _ => println("Partitioning Err: "+key.getClass);1;
    }

    override def equals(other: Any): Boolean = other match {
      case h: IDPartitioner =>
        h.numPartitions == numPartitions
      case _ =>
        false
    }

    override def hashCode: Int = numPartitions
  }
}
