package it.polimi.genomics.spark.implementation.RegionsOperators


import it.polimi.genomics.core.DataStructures.RegionAggregate.{COORD_POS, RegionExtension}
import it.polimi.genomics.core.DataStructures.RegionCondition.MetaAccessor
import it.polimi.genomics.core.DataStructures.{MetaOperator, RegionOperator}
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core._
import it.polimi.genomics.core.exception.SelectFormatException
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
  def apply(executor : GMQLSparkExecutor, projectedValues : Option[List[Int]], tupleAggregator : Option[List[RegionExtension]], inputDataset : RegionOperator, inputMeta: MetaOperator, env : SparkContext) : RDD[GRECORD] = {
    logger.info("----------------ProjectRD executing..")

    val input = executor.implement_rd(inputDataset, env)
    val metadata = executor.implement_md(inputMeta, env)
    val meta: Array[(Long, (String, String))]  = if (tupleAggregator.isDefined) {
      val metaaccessors = tupleAggregator.get.flatMap { agg =>
        agg.inputIndexes.flatMap(in =>
          if (in.isInstanceOf[MetaAccessor]) Some(in.asInstanceOf[MetaAccessor].attribute_name) else None
        )
      }

      if (metaaccessors.size > 0)
        metadata.filter(x => metaaccessors.contains(x._2._1)).distinct.collect
      else
        Array[(Long, (String, String))]()
    } else Array[(Long, (String, String))]()

    val extended = if (tupleAggregator.isDefined) input.flatMap { a =>


      extendRegion(a, a, tupleAggregator.get,meta)
    }.cache()
    else input

    if(projectedValues.isDefined)
      extended.map(a  => (a._1,  projectedValues.get.foldLeft(Array[GValue]())((Acc, b) => Acc :+ a._2(b)) ))
    /*else extended*/
    else if (tupleAggregator.isDefined)
      extended
    else
      extended.map(a=> (a._1, new Array[GValue](0)))
  }

  def computeFunction(r : GRECORD, agg : RegionExtension,inputMeta:Array[(Long,(String,String))]) : GValue = {
    agg.fun( agg.inputIndexes.foldLeft(Array[GValue]())((acc,b) => acc :+ {
      if(b.isInstanceOf[MetaAccessor]){
        val values = inputMeta.filter(x=>
          x._1
            == r._1._1
          && x._2._1.equals(b.asInstanceOf[MetaAccessor].attribute_name)).distinct

        if (values.size > 0) {
          val value = values.head._2._2
          try {
            GDouble(value.toDouble)
          }
          catch {
            case _: Throwable => GString(value)
          }
        } else GNull()
      }else b.asInstanceOf[Int] match {
        case COORD_POS.CHR_POS => new GString(r._1._2)
        case COORD_POS.LEFT_POS => new GDouble(r._1._3)
        case COORD_POS.RIGHT_POS => new GDouble(r._1._4)
        case COORD_POS.STRAND_POS => new GString(r._1._5.toString)
        case _: Int => r._2(b.asInstanceOf[Int])
      }
    }))
  }

  def extendRegion(out : GRECORD, r:GRECORD, aggList : List[RegionExtension],inputMeta:Array[(Long,(String,String))]) : Option[GRECORD] = {
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
        case Some(COORD_POS.CHR_POS) => extendRegion((new GRecordKey(out._1._1, computeFunction(r, agg,inputMeta).asInstanceOf[GString].v, out._1._3, out._1._4, out._1._5), out._2),r, aggList.tail,inputMeta)
        case Some(COORD_POS.LEFT_POS) => extendRegion((new GRecordKey(out._1._1, out._1._2, computeFunction(r, agg,inputMeta).asInstanceOf[GDouble].v.toLong, out._1._4, out._1._5), out._2),r, aggList.tail,inputMeta)
        case Some(COORD_POS.RIGHT_POS) => extendRegion((new GRecordKey(out._1._1, out._1._2, out._1._3, computeFunction(r, agg,inputMeta).asInstanceOf[GDouble].v.toLong, out._1._5), out._2),r, aggList.tail,inputMeta)
        case Some(COORD_POS.STRAND_POS) => extendRegion((new GRecordKey(out._1._1, out._1._2, out._1._3, out._1._4, computeFunction(r, agg,inputMeta).asInstanceOf[GString].v.charAt(0)), out._2),r, aggList.tail,inputMeta)
        case Some(COORD_POS.START_POS) => {
          if (out._1._5.equals('-')) {
            extendRegion((new GRecordKey(out._1._1, out._1._2, out._1._3, computeFunction(r, agg,inputMeta).asInstanceOf[GDouble].v.toLong, out._1._5), out._2), r, aggList.tail,inputMeta)
          } else
            extendRegion((new GRecordKey(out._1._1, out._1._2, computeFunction(r, agg,inputMeta).asInstanceOf[GDouble].v.toLong, out._1._4, out._1._5), out._2),r, aggList.tail,inputMeta)
        }
        case Some(COORD_POS.STOP_POS) => {
          if (out._1._5.equals('-')) {
            extendRegion((new GRecordKey(out._1._1, out._1._2, computeFunction(r, agg,inputMeta).asInstanceOf[GDouble].v.toLong, out._1._4, out._1._5), out._2), r, aggList.tail,inputMeta)
          } else
            extendRegion((new GRecordKey(out._1._1, out._1._2, out._1._3, computeFunction(r, agg,inputMeta).asInstanceOf[GDouble].v.toLong, out._1._5), out._2),r, aggList.tail,inputMeta)
        }
        case Some(v : Int) => extendRegion((out._1, {out._2.update(v, computeFunction(r, agg,inputMeta)); out._2} ),r, aggList.tail,inputMeta)
        case None => extendRegion((out._1, out._2 :+ computeFunction(r, agg,inputMeta)),r, aggList.tail,inputMeta)
      }
    }
  }
}