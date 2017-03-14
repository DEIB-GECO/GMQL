package it.polimi.genomics.spark.implementation.RegionsOperators


import it.polimi.genomics.core.DataStructures.RegionAggregate.{COORD_POS, RegionExtension}
import it.polimi.genomics.core.DataStructures.RegionOperator
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.core.{GDouble, GRecordKey, GString, GValue}
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

/**
 * Created by abdulrahman kaitoua on 06/07/15.
 */
object ProjectRD {
  private final val logger = LoggerFactory.getLogger(this.getClass);

  @throws[SelectFormatException]
  def apply(executor : GMQLSparkExecutor, projectedValues : Option[List[Int]], tupleAggregator : Option[List[RegionExtension]], inputDataset : RegionOperator, env : SparkContext) : RDD[GRECORD] = {
    logger.info("----------------ProjectRD executing..")

    val input = executor.implement_rd(inputDataset, env)

   val extended = if (tupleAggregator.isDefined) input.flatMap { a =>
      extendRegion(a, a, tupleAggregator.get)
    }.cache()
    else input

    if(projectedValues.isDefined)
      extended.map(a  => (a._1,  projectedValues.get.foldLeft(Array[GValue]())((Acc, b) => Acc :+ a._2(b)) ))
    else extended
  }

  def computeFunction(r : GRECORD, agg : RegionExtension) : GValue = {
    agg.fun( agg.inputIndexes.foldLeft(Array[GValue]())((acc,b) => acc :+ {
      b match {
        case COORD_POS.CHR_POS => new GString(r._1._2)
        case COORD_POS.LEFT_POS => new GDouble(r._1._3)
        case COORD_POS.RIGHT_POS => new GDouble(r._1._4)
        case COORD_POS.STRAND_POS => new GString(r._1._5.toString)
        case _ : Int => r._2(b)
      }
    }))
  }

  def extendRegion(out : GRECORD, r:GRECORD, aggList : List[RegionExtension]) : Option[GRECORD] = {
    if(aggList.isEmpty) {
      //out
      if (out._1._3 >= out._1._4) // if left > right, the region is deleted
      {
        None
      }
      else if (out._1._3 < 0) //if left become < 0, set it to 0
      {
        Some((new GRecordKey(out._1._1, out._1._2, 0, out._1._4, out._1._5), out._2))
      }
      else Some(out)
    }
    else {
      val agg = aggList.head
      agg.output_index match {
        case Some(COORD_POS.CHR_POS) => extendRegion((new GRecordKey(out._1._1, computeFunction(r, agg).asInstanceOf[GString].v, out._1._3, out._1._4, out._1._5), out._2),r, aggList.tail)
        case Some(COORD_POS.LEFT_POS) => extendRegion((new GRecordKey(out._1._1, out._1._2, computeFunction(r, agg).asInstanceOf[GDouble].v.toLong, out._1._4, out._1._5), out._2),r, aggList.tail)
        case Some(COORD_POS.RIGHT_POS) => extendRegion((new GRecordKey(out._1._1, out._1._2, out._1._3, computeFunction(r, agg).asInstanceOf[GDouble].v.toLong, out._1._5), out._2),r, aggList.tail)
        case Some(COORD_POS.STRAND_POS) => extendRegion((new GRecordKey(out._1._1, out._1._2, out._1._3, out._1._4, computeFunction(r, agg).asInstanceOf[GString].v.charAt(0)), out._2),r, aggList.tail)
        case Some(COORD_POS.START_POS) => {
          if (out._1._5.equals('-')) {
            extendRegion((new GRecordKey(out._1._1, out._1._2, out._1._3, computeFunction(r, agg).asInstanceOf[GDouble].v.toLong, out._1._5), out._2), r, aggList.tail)
          } else
            extendRegion((new GRecordKey(out._1._1, out._1._2, computeFunction(r, agg).asInstanceOf[GDouble].v.toLong, out._1._4, out._1._5), out._2),r, aggList.tail)
        }
        case Some(COORD_POS.STOP_POS) => {
          if (out._1._5.equals('-')) {
            extendRegion((new GRecordKey(out._1._1, out._1._2, computeFunction(r, agg).asInstanceOf[GDouble].v.toLong, out._1._4, out._1._5), out._2), r, aggList.tail)
          } else
            extendRegion((new GRecordKey(out._1._1, out._1._2, out._1._3, computeFunction(r, agg).asInstanceOf[GDouble].v.toLong, out._1._5), out._2),r, aggList.tail)
        }
        case Some(v : Int) => extendRegion((out._1, {out._2.update(v, computeFunction(r, agg)); out._2} ),r, aggList.tail)
        case None => extendRegion((out._1, out._2 :+ computeFunction(r, agg)),r, aggList.tail)
      }
    }
  }
}