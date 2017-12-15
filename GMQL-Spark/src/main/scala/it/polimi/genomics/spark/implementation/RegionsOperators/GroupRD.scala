package it.polimi.genomics.spark.implementation.RegionsOperators

import com.google.common.hash.Hashing
import it.polimi.genomics.core.DataStructures.GroupRDParameters.FIELD
import it.polimi.genomics.core.DataStructures.{GroupRDParameters, RegionAggregate, RegionOperator}
import it.polimi.genomics.core.DataTypes.GRECORD
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.core.{GNull, GValue}
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

/**
 * Created by abdulrahman kaitoua on 14/07/15.
 */
object GroupRD {
  private final val logger = LoggerFactory.getLogger(this.getClass);


  @throws[SelectFormatException]
  def apply(executor : GMQLSparkExecutor, groupingParameters : Option[List[GroupRDParameters.GroupingParameter]], aggregates : Option[List[RegionAggregate.RegionsToRegion]], regionDataset : RegionOperator, sc : SparkContext) : RDD[GRECORD] = {
    logger.info("----------------GroupRD executing..")

    val ds = executor.implement_rd(regionDataset, sc)

    val res : RDD[GRECORD] =
      ds.map(r  => (r._1, r._2.map((g : GValue) => List(g))))
        .groupBy{ r =>
          val hasher = Hashing.md5.newHasher
          hasher.putString(r._1._1.toString,java.nio.charset.Charset.defaultCharset())
          hasher.putString(r._1._2.toString,java.nio.charset.Charset.defaultCharset())
          hasher.putString(r._1._3.toString,java.nio.charset.Charset.defaultCharset())
          hasher.putString(r._1._4.toString,java.nio.charset.Charset.defaultCharset())
          hasher.putString(r._1._5.toString,java.nio.charset.Charset.defaultCharset())
          if(groupingParameters.isDefined){
            groupingParameters.get.foreach{case FIELD(pos) => hasher.putString("§",java.nio.charset.Charset.defaultCharset()).putString(r._2(pos).mkString("§"),java.nio.charset.Charset.defaultCharset())}
          }
          hasher.hash.asLong()
        }
        .map(x=>x._2.reduce{(a , b) => (a._1, a._2.zip(b._2).map((c) => c._1 ++ c._2))})
        //applying functions
        .map{a =>
          val aggregated : Array[GValue] =
            if(aggregates.isDefined){
              aggregates.get.foldLeft(new Array[GValue](0)){(z, agg) =>
                val fun = agg.fun(a._2(agg.index))
                val count = (a._2(agg.index).length, a._2(agg.index).foldLeft(0)((x,y)=> if (y.isInstanceOf[GNull]) x+0 else x+1))
                z :+ agg.funOut(fun,count)}
            } else {
              new Array[GValue](0)
            }

          val groupingFields: Array[GValue] = if (groupingParameters.isDefined){
            groupingParameters.get.map{ case FIELD(pos) =>
              a._2(pos)
            }.toArray.map(f => f.head)
          } else new Array[GValue](0)


          (a._1, groupingFields ++ aggregated)
        }
    res
  }

}
