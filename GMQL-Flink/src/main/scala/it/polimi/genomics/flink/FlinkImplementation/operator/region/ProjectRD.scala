package it.polimi.genomics.flink.FlinkImplementation.operator.region

import it.polimi.genomics.core.DataStructures.RegionAggregate.{COORD_POS, RegionExtension}
import it.polimi.genomics.core.DataStructures.RegionOperator
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.{GDouble, GRecordKey, GString, GValue}
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.flink.FlinkImplementation.FlinkImplementation
import org.apache.flink.api.scala._
import org.slf4j.LoggerFactory

/**
 * Created by michelebertoni on 06/05/15.
 */
object ProjectRD {

  final val logger = LoggerFactory.getLogger(this.getClass)
  @throws[SelectFormatException]
  def apply(executor : FlinkImplementation, projectedValues : Option[List[Int]], tupleAggregator : Option[List[RegionExtension]], inputDataset : RegionOperator, env : ExecutionEnvironment) : DataSet[FlinkRegionType] = {
    val input = executor.implement_rd(inputDataset, env)

    val distinctInput = input
    /*
    val distinctInput : DataSet[(Long, String, Long, Long, Char, Array[GValue])] =
      if(distinctValues.isDefined){
        val s = new StringBuilder
        val hf : HashFunction = Hashing.sha256()
        input.distinct((a : FlinkRegionType) => {
          s.setLength(0)
          s.append(a._1)
          s.append(a._2)
          s.append(a._3)
          s.append(a._4)
          s.append(a._5)
          distinctValues.get.map((b : Int) => s.append(a._6(b)))
          hf.newHasher.putString(s.toString(), Charsets.UTF_8).hash.asLong()
        })
      } else {
        input
      }
    */

    val projectedInput : DataSet[(Long, String, Long, Long, Char, Array[GValue])] =
      if(projectedValues.isDefined){
        distinctInput.map((a : FlinkRegionType) => (a._1, a._2, a._3, a._4, a._5, projectedValues.get.foldLeft(new Array[GValue](0))((z, b) => z :+ a._6(b)) ))
      } else {
        distinctInput
      }

    if(tupleAggregator.isDefined){
      val agg =
        tupleAggregator.get
      projectedInput.flatMap((a : FlinkRegionType) => {
        extendRegion(a, agg)
      })
    } else {
      projectedInput
    }

  }

  def computeFunction(r : FlinkRegionType, agg : RegionExtension) : GValue = {
    agg.fun( agg.inputIndexes.foldLeft(new Array[GValue](0))((z,b) => z :+ {
      b.asInstanceOf[Int] match {
        case COORD_POS.CHR_POS => new GString(r._2)
        case COORD_POS.LEFT_POS => new GDouble(r._3)
        case COORD_POS.RIGHT_POS => new GDouble(r._4)
        case COORD_POS.STRAND_POS => new GString(r._5.toString)
        case _ : Int => r._6(b.asInstanceOf[Int])
      }
    }) )
  }

  def extendRegion(r : FlinkRegionType, aggList : List[RegionExtension]) : Option[FlinkRegionType]= {
    if(aggList.isEmpty){
      //r
      if (r._3 >= r._4) // if left > right, the region is deleted
      {
        None
      }
      else if (r._3 < 0) //if left become < 0, set it to 0
      {
        Some((r._1, r._2, 0, r._4, r._5, r._6))
      }
      else Some(r)
    } else {
      val agg = aggList.head
      agg.output_index match {
        case Some(COORD_POS.CHR_POS) => extendRegion((r._1, computeFunction(r, agg).asInstanceOf[GString].v, r._3, r._4, r._5, r._6), aggList.drop(1))
        case Some(COORD_POS.LEFT_POS) => extendRegion((r._1, r._2, computeFunction(r, agg).asInstanceOf[GDouble].v.toLong, r._4, r._5, r._6), aggList.drop(1))
        case Some(COORD_POS.RIGHT_POS) => extendRegion((r._1, r._2, r._3, computeFunction(r, agg).asInstanceOf[GDouble].v.toLong, r._5, r._6), aggList.drop(1))
        case Some(COORD_POS.STRAND_POS) => extendRegion((r._1, r._2, r._3, r._4, computeFunction(r, agg).asInstanceOf[GString].v.charAt(0), r._6), aggList.drop(1))
        case Some(COORD_POS.START_POS) => {
          if (r._5.equals('-')) {
            extendRegion((r._1, r._2, r._3, computeFunction(r, agg).asInstanceOf[GDouble].v.toLong, r._5, r._6), aggList.drop(1))
          } else
            extendRegion((r._1, r._2, computeFunction(r, agg).asInstanceOf[GDouble].v.toLong, r._4, r._5, r._6), aggList.drop(1))
        }
        case Some(COORD_POS.STOP_POS) => {
          if (r._5.equals('-')) {
            extendRegion((r._1, r._2, computeFunction(r, agg).asInstanceOf[GDouble].v.toLong, r._4, r._5, r._6), aggList.drop(1))
          } else
            extendRegion((r._1, r._2, r._3, computeFunction(r, agg).asInstanceOf[GDouble].v.toLong, r._5, r._6), aggList.drop(1))
        }
        case Some(v : Int) => extendRegion((r._1, r._2, r._3, r._4, r._5, {r._6.update(v, computeFunction(r, agg)); r._6} ), aggList.drop(1))
        case None => extendRegion((r._1, r._2, r._3, r._4, r._5, r._6 :+ computeFunction(r, agg)), aggList.drop(1))
      }
    }
  }

}