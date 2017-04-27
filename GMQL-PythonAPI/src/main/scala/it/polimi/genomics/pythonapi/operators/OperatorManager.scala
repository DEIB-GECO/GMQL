package it.polimi.genomics.pythonapi.operators

import it.polimi.genomics.core.DataStructures.CoverParameters._
import it.polimi.genomics.core.DataStructures.CoverParameters.CoverFlag.CoverFlag
import it.polimi.genomics.core.DataStructures.JoinParametersRD._
import it.polimi.genomics.core.DataStructures.MetaAggregate.MetaAggregateStruct
import it.polimi.genomics.core.DataStructures.JoinParametersRD.RegionBuilder.RegionBuilder
import it.polimi.genomics.core.DataStructures.MetaJoinCondition.{AttributeEvaluationStrategy, Default, MetaJoinCondition}
import it.polimi.genomics.core.DataStructures.MetadataCondition.MetadataCondition
import it.polimi.genomics.core.DataStructures.RegionAggregate.{RegionFunction, RegionsToRegion}
import it.polimi.genomics.core.DataStructures.RegionCondition.RegionCondition
import it.polimi.genomics.pythonapi.PythonManager
import scala.collection.JavaConversions._

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
  * Created by Luca Nanni on 11/04/17.
  * Email: luca.nanni@mail.polimi.it
  */
object OperatorManager {


  /*
  * SELECT
  * */
  def meta_select(index: Int, metaCondition : MetadataCondition) : Int =
  {
    // get the corresponding variable
    val v = PythonManager.getVariable(index)
    // do the operation (build the DAG)
    val nv = v SELECT metaCondition
    // generate new index
    val new_index = PythonManager.putNewVariable(nv)
    new_index
  }

  def reg_select(index: Int, regionCondition: RegionCondition) : Int =
  {
    // get the corresponding variable
    val v = PythonManager.getVariable(index)
    // do the operation (build the DAG)
    val nv = v SELECT regionCondition
    // generate new index
    val new_index = PythonManager.putNewVariable(nv)
    new_index
  }

  def select(index: Int, metaCondition : MetadataCondition, regionCondition: RegionCondition) : Int = ???


  /*
  * PROJECT
  * */
  def meta_project(index : Int, projected_meta: java.util.List[String]): Int = {
    do_project(index, Option(projected_meta), None, None, None)
  }

  def meta_project_extend(index : Int, projected_meta: java.util.List[String],
                          extended_meta : MetaAggregateStruct): Int = {
    do_project(index, Option(projected_meta), Option(extended_meta), None, None)
  }

  def reg_project(index : Int, projected_regs : java.util.List[String]): Int = {
    do_project(index, None, None, Option(projected_regs), None)
  }

  def reg_project_extend(index : Int, projected_regs : java.util.List[String],
                         extended_regs : java.util.List[RegionFunction]): Int = {
    do_project(index, None, None, Option(projected_regs), Option(extended_regs))
  }

  def do_project(index : Int, projected_meta : Option[java.util.List[String]], extended_meta : Option[MetaAggregateStruct],
                 projected_regs : Option[java.util.List[String]], extended_regs : Option[java.util.List[RegionFunction]]): Int =
  {
    // get the corresponding variable
    val v = PythonManager.getVariable(index)

    var projected_meta_values : Option[List[String]] = None
    if(projected_meta.isDefined) {
      projected_meta_values = Option(projected_meta.get.asScala.toList)
    }

    var projected_regs_values : Option[List[Int]] = None
    if(projected_regs.isDefined) {
      val l = projected_regs.get.asScala.toList
      val proj = new ListBuffer[Int]()
      for(p <- l) {
        val n = v.get_field_by_name(p)
        if(n.isDefined){
          proj += n.get
        }
      }
      projected_regs_values = Option(proj.toList)
    }

    var extended_regs_l : Option[List[RegionFunction]] = None
    if(extended_regs.isDefined) {
      val extnd = new ListBuffer[RegionFunction]()
      val l = extended_regs.get.asScala.toList
      for(r <- l) {
        extnd += r
      }
      extended_regs_l = Option(extnd.toList)
    }


    // do the operation
    val nv = v PROJECT(projected_meta_values,
      extended_meta, projected_regs_values,
      extended_regs_l)

    // generate new index
    val new_index = PythonManager.putNewVariable(nv)
    new_index
  }

  /*
  * COVER
  * */

  def getCoverTypes(name : String): CoverFlag = {
    name match {
      case "normal" => CoverFlag.COVER
      case "flat" => CoverFlag.FLAT
      case "summit" => CoverFlag.SUMMIT
      case "histogram" => CoverFlag.HISTOGRAM
      case _ => CoverFlag.COVER
    }
  }

  def getCoverParam(p : String): CoverParam = {
    p match {
      case "ALL" => ALL()
      case "ANY" => ANY()
      case _ => {
        val number = p.toInt
        N(number)
      }
    }
  }

  def cover(index: Int, coverFlag : CoverFlag, minAcc : CoverParam, maxAcc : CoverParam,
            groupBy : java.util.List[String], aggregates: java.util.List[RegionsToRegion]): Int = {
    val groupByPar : Option[List[String]] = {
      if(groupBy.size() > 0)
        Option(groupBy.asScala.toList)
      else
        None
    }
    val aggregatesPar = aggregates.asScala.toList

    val v = PythonManager.getVariable(index)
    val nv = v COVER(coverFlag,minAcc,maxAcc,aggregatesPar,groupByPar)
    // generate new index
    val new_index = PythonManager.putNewVariable(nv)
    new_index
  }

  def getGenometricCondition(conditionName : String, argument : String): AtomicCondition = {
    conditionName match {
      case "DLE" => DistLess(argument.toLong)
      case "DGE" => DistGreater(argument.toLong)
      case "MD" => MinDistance(argument.toInt)
      case "UP" => Upstream()
      case "DOWN" => DownStream()
      case _ => DistLess(1000)
    }
  }

  /*
  * JOIN
  * */

  def getRegionJoinCondition(atomicConditionsList : java.util.List[AtomicCondition]): List[JoinQuadruple] = {
    atomicConditionsList.size() match {
      case 0 => List(JoinQuadruple(Option(DistLess(Long.MaxValue))))
      case 1 => List(JoinQuadruple(Option(atomicConditionsList.get(0))))
      case 2 => List(JoinQuadruple(Option(atomicConditionsList.get(0)), Option(atomicConditionsList.get(1))))
      case 3 => List(JoinQuadruple(Option(atomicConditionsList.get(0)), Option(atomicConditionsList.get(1)),
        Option(atomicConditionsList.get(2))))
      case 4 => List(JoinQuadruple(Option(atomicConditionsList.get(0)), Option(atomicConditionsList.get(1)),
        Option(atomicConditionsList.get(2)), Option(atomicConditionsList.get(3))))
      case _ => List(JoinQuadruple(Option(atomicConditionsList.get(0)), Option(atomicConditionsList.get(1)),
        Option(atomicConditionsList.get(2)), Option(atomicConditionsList.get(3))))
    }
  }

  def getMetaJoinCondition(metadataAttributeList : java.util.List[String]): Option[MetaJoinCondition] = {
    // for now we consider only DEFAULT AttributeEvaluationStrategy
    var listAttributes = new ListBuffer[AttributeEvaluationStrategy]()

    for(m <- metadataAttributeList) {
      val att = Default(m)
      listAttributes += att
    }
    if(listAttributes.size < 1)
      None
    else
      Option(MetaJoinCondition(listAttributes.toList))
  }

  def getRegionBuilderJoin(builder: String) = {
    builder match {
      case "LEFT" =>  RegionBuilder.LEFT
      case "RIGHT" => RegionBuilder.RIGHT
      case "INT" => RegionBuilder.INTERSECTION
      case "CONTIG" => RegionBuilder.CONTIG
      case _ => RegionBuilder.LEFT
    }
  }

  def join(index: Int, other: Int, metaJoinCondition: Option[MetaJoinCondition],
           regionJoinCondition: List[JoinQuadruple], regionBuilder : RegionBuilder): Int = {
    val v = PythonManager.getVariable(index)
    val other_v = PythonManager.getVariable(other)

    val nv = v.JOIN(metaJoinCondition,regionJoinCondition,regionBuilder,other_v)
    // generate new index
    val new_index = PythonManager.putNewVariable(nv)
    new_index
  }

  /*
  * MAP
  * */
  def map(index: Int, other: Int, metaJoinCondition: Option[MetaJoinCondition],
          aggregates: java.util.List[RegionsToRegion]): Int = {
    val v = PythonManager.getVariable(index)
    val other_v = PythonManager.getVariable(other)

    val nv = v.MAP(metaJoinCondition,aggregates.toList,other_v)
    // generate new index
    val new_index = PythonManager.putNewVariable(nv)
    new_index
  }
}
