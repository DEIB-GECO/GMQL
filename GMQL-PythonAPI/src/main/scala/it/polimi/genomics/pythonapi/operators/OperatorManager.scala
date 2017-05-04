package it.polimi.genomics.pythonapi.operators

import it.polimi.genomics.core.DataStructures.CoverParameters._
import it.polimi.genomics.core.DataStructures.CoverParameters.CoverFlag.CoverFlag
import it.polimi.genomics.core.DataStructures.GroupMDParameters.Direction.{Direction, ASC, DESC}
import it.polimi.genomics.core.DataStructures.GroupMDParameters.{NoTop, Top, TopG, TopParameter}
import it.polimi.genomics.core.DataStructures.JoinParametersRD._
import it.polimi.genomics.core.DataStructures.MetaAggregate.MetaAggregateStruct
import it.polimi.genomics.core.DataStructures.JoinParametersRD.RegionBuilder.RegionBuilder
import it.polimi.genomics.core.DataStructures.MetaJoinCondition.{AttributeEvaluationStrategy, Default, MetaJoinCondition}
import it.polimi.genomics.core.DataStructures.MetadataCondition.MetadataCondition
import it.polimi.genomics.core.DataStructures.RegionAggregate.{RegionFunction, RegionsToRegion}
import it.polimi.genomics.core.DataStructures.RegionCondition.RegionCondition
import it.polimi.genomics.pythonapi.PythonManager
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
  * Created by Luca Nanni on 11/04/17.
  * Email: luca.nanni@mail.polimi.it
  */
object OperatorManager {

  private val logger = LoggerFactory.getLogger(this.getClass)
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
      case "DLE" =>
        logger.info("DLE("+argument.toLong+")")
        DistLess(argument.toLong)
      case "DGE" =>
        logger.info("DGE("+argument.toLong+")")
        DistGreater(argument.toLong)
      case "MD" =>
        logger.info("MD("+argument.toInt+")")
        MinDistance(argument.toInt)
      case "UP" =>
        logger.info("UP")
        Upstream()
      case "DOWN" =>
        logger.info("DOWN")
        DownStream()
      case _ => throw new IllegalArgumentException(conditionName + " is not a Genometric condition")
    }
  }

  /*
  * JOIN
  * */

  def getRegionJoinCondition(atomicConditionsList : java.util.List[AtomicCondition]): List[JoinQuadruple] = {
    atomicConditionsList.size() match {
      case 0 =>
        logger.info("Empty JoinQuadruple")
        List(JoinQuadruple(Option(DistLess(Long.MaxValue))))
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
      case _ => throw new IllegalArgumentException(builder + " is not a region builder")
    }
  }

  def join(index: Int, other: Int, metaJoinCondition: Option[MetaJoinCondition],
           regionJoinCondition: List[JoinQuadruple], regionBuilder : RegionBuilder,
           referenceName: String, experimentName: String): Int = {

    var refName : Option[String] = None
    var expName : Option[String] = None

    if(referenceName.length() > 0)
      refName = Option(referenceName)

    if(experimentName.length() > 0)
      expName = Option(experimentName)

    val v = PythonManager.getVariable(index)
    val other_v = PythonManager.getVariable(other)

    val nv = v.JOIN(metaJoinCondition,regionJoinCondition,regionBuilder,other_v,refName,expName)
    // generate new index
    val new_index = PythonManager.putNewVariable(nv)
    new_index
  }

  /*
  * MAP
  * */
  def map(index: Int, other: Int, metaJoinCondition: Option[MetaJoinCondition],
          aggregates: java.util.List[RegionsToRegion],
          referenceName: String, experimentName: String): Int = {

    var refName : Option[String] = None
    var expName : Option[String] = None

    if(referenceName.length() > 0)
      refName = Option(referenceName)

    if(experimentName.length() > 0)
      expName = Option(experimentName)

    val v = PythonManager.getVariable(index)
    val other_v = PythonManager.getVariable(other)

    val nv = v.MAP(metaJoinCondition,aggregates.toList,other_v, refName, expName)
    // generate new index
    val new_index = PythonManager.putNewVariable(nv)
    new_index
  }

  /*
  * ORDER
  * */

  def getOrderTopParameter(paramName : String, k : String) : TopParameter = {
    paramName.toUpperCase match {
      case "TOP" => Top(k.toInt)
      case "TOPG" => TopG(k.toInt)
      case _ => NoTop()
    }
  }

  def getOrderDirection(value: Boolean): Direction = {
    if(value){
      ASC
    }
    else
      DESC
  }

  def order(index: Int, meta_ordering : java.util.List[String], meta_ascending : java.util.List[Boolean],
           metaTop : String, metaK : String, region_ordering : java.util.List[String], region_ascending: java.util.List[Boolean],
            regionTop: String, regionK : String): Int = {
    val metaTopParameter = getOrderTopParameter(metaTop, metaK)
    val regionTopParameter = getOrderTopParameter(regionTop, regionK)

    var metaOrdering : Option[List[(String,Direction)]] = None
    var regionOrdering : Option[List[(Int,Direction)]] = None

    val v = PythonManager.getVariable(index)

    if(meta_ordering.size() > 0){
      val result = new ListBuffer[(String,Direction)]()
      if(meta_ascending.size() > 0) {
        for(i <- 0 to meta_ascending.size()){
          val meta = meta_ordering.get(i)
          val direction = getOrderDirection(meta_ascending.get(i))
          result += Tuple2(meta, direction)
        }
      }
      else {
        for(i <- 0 to meta_ascending.size()){
          val meta = meta_ordering.get(i)
          val direction = getOrderDirection(true)
          result += Tuple2(meta, direction)
        }
      }
      metaOrdering = Option(result.toList)
    }
    if(region_ordering.size() > 0){
      val result = new ListBuffer[(Int,Direction)]()
      if(region_ascending.size() > 0) {
        for(i <- 0 to region_ascending.size()){
          val regionName = region_ordering.get(i)
          val regionNumber = v.get_field_by_name(regionName).get
          val direction = getOrderDirection(region_ascending.get(i))
          result += Tuple2(regionNumber, direction)
        }
      }
      else {
        for(i <- 0 to region_ascending.size()){
          val regionName = region_ordering.get(i)
          val regionNumber = v.get_field_by_name(regionName).get
          val direction = getOrderDirection(true)
          result += Tuple2(regionNumber, direction)
        }
      }
      regionOrdering = Option(result.toList)
    }

    val nv = v.ORDER(metaOrdering,"_group",metaTopParameter,regionOrdering,regionTopParameter)

    // generate new index
    val new_index = PythonManager.putNewVariable(nv)
    new_index
  }
}
