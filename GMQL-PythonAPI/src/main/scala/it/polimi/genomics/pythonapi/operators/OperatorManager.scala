package it.polimi.genomics.pythonapi.operators

import it.polimi.genomics.core.DataStructures.CoverParameters._
import it.polimi.genomics.core.DataStructures.CoverParameters.CoverFlag.CoverFlag
import it.polimi.genomics.core.DataStructures.GroupMDParameters.Direction.{ASC, DESC, Direction}
import it.polimi.genomics.core.DataStructures.GroupMDParameters.{NoTop, Top, TopG, TopParameter}
import it.polimi.genomics.core.DataStructures.IRVariable
import it.polimi.genomics.core.DataStructures.JoinParametersRD._
import it.polimi.genomics.core.DataStructures.MetaAggregate.{MetaAggregateStruct, MetaExtension}
import it.polimi.genomics.core.DataStructures.JoinParametersRD.RegionBuilder.RegionBuilder
import it.polimi.genomics.core.DataStructures.MetaJoinCondition.{AttributeEvaluationStrategy, Default, MetaJoinCondition}
import it.polimi.genomics.core.DataStructures.MetadataCondition.MetadataCondition
import it.polimi.genomics.core.DataStructures.RegionAggregate.{RegionFunction, RegionsToMeta, RegionsToRegion}
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


/**
  * This object manages the creation of the DAG. Every GMQL operation defined for an IRVariable is wrapped
  * in one or more of the following functions. The protocol that is used is the following:
  *
  *   new_variable_index = OPERATION(variable_index, ...)
  *
  * where the indices are the key of the map {index --> IRVariable} of PythonManager.
  * */
object OperatorManager {

  private val logger = LoggerFactory.getLogger(this.getClass)
  /*
  * SELECT
  * */

  def meta_select(index: Int, other: Int, metaCondition: MetadataCondition): Int = {
    meta_select(index, other, metaCondition, None)
  }

  def meta_select(index: Int, other: Int,  metaCondition : MetadataCondition,
                  metaJoinCondition: Option[MetaJoinCondition]) : Int = {


    // get the corresponding variable
    val v = PythonManager.getVariable(index)
    // do the operation (build the DAG)
    var nv : IRVariable = null
    if(other >= 0 && metaJoinCondition.isDefined) {
      val other_v = PythonManager.getVariable(other)

      nv = v SELECT(metaJoinCondition.get, other_v, metaCondition)
    }
    else if(other < 0 && metaJoinCondition.isEmpty) {
      nv = v SELECT metaCondition
    }
    else
      throw new IllegalArgumentException("other_index >= 0 <=> metaJoinCondition")

    // generate new index
    val new_index = PythonManager.putNewVariable(nv)
    new_index
  }

  def reg_select(index: Int, other: Int, regionCondition: RegionCondition): Int = {
    reg_select(index, other, regionCondition, None)
  }

  def reg_select(index: Int, other: Int, regionCondition: RegionCondition,
                 metaJoinCondition: Option[MetaJoinCondition]) : Int = {
    // get the corresponding variable
    val v = PythonManager.getVariable(index)
    // do the operation (build the DAG)
    var nv : IRVariable = null
    if(other >= 0 && metaJoinCondition.isDefined) {
      val other_v = PythonManager.getVariable(other)
      nv = v SELECT(metaJoinCondition.get, other_v, regionCondition)
    }
    else if(other < 0 && metaJoinCondition.isEmpty) {
      nv = v SELECT regionCondition
    }
    else
      throw new IllegalArgumentException("other_index >= 0 <=> metaJoinCondition")
    // generate new index
    val new_index = PythonManager.putNewVariable(nv)
    new_index
  }

  def only_semi_select(index: Int, other:Int, metaJoinCondition: Option[MetaJoinCondition]): Int = {
    // get the corresponding variable
    val v = PythonManager.getVariable(index)
    var nv : IRVariable = null
    if(other >= 0 && metaJoinCondition.isDefined) {
      val other_v = PythonManager.getVariable(other)
      nv = v SELECT(metaJoinCondition.get, other_v)
    }
    else {
      throw new IllegalArgumentException("other_index >= 0 && metaJoinCondition")
    }
    // generate new index
    val new_index = PythonManager.putNewVariable(nv)
    new_index
  }

  /*
  * PROJECT
  * */

  def project(index: Int, projected_meta : Option[java.util.List[String]],
              extended_meta : Option[MetaExtension],
              projected_regs : Option[java.util.List[String]],
              all_but : Option[java.util.List[String]],
              extended_regs : Option[java.util.List[RegionFunction]]
             ): Int = {
    val v = PythonManager.getVariable(index)

    // PROJECTED META
    var projected_meta_scala: Option[List[String]] = None
    if(projected_meta.isDefined){
      projected_meta_scala = Some(projected_meta.get.asScala.toList)
    }
    // ALL BUT
    var all_but_scala: Option[List[String]] = None
    if(all_but.isDefined){
      all_but_scala = Some(all_but.get.asScala.toList)
    }

    // PROJECTED REGIONS
    var projected_regs_scala: Option[List[Int]] = None
    if(projected_regs.isDefined){
      projected_regs_scala = Some(projected_regs.get.asScala.toList.map( x => {
        val pos = v.get_field_by_name(x)
        if(pos.isDefined){
          pos.get
        }
        else
          throw new IllegalArgumentException("The attribute " + x + " is not present")
      }))
    }

    // EXTENDED REGIONS
    var extended_regs_scala: Option[List[RegionFunction]] = None
    if(extended_regs.isDefined){
      extended_regs_scala = Some(extended_regs.get.asScala.toList)
    }

    val nv = v.PROJECT(projected_meta_scala, extended_meta,
      projected_regs_scala,all_but_scala, extended_regs_scala)

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
      case "ALL" => new ALL{}
      case "ANY" => new ANY{}
      case _ =>
        val number = p.toInt
        new N {
          override val n: Int = number

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

  def getRegionBuilderJoin(builder: String) : RegionBuilder = {
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

  /*
  * EXTEND
  * */
  def extend(index: Int, region_aggregates: java.util.List[RegionsToMeta]): Int = {
    // get the corresponding variable
    val v = PythonManager.getVariable(index)
    val nv = v.EXTEND(region_aggregates.asScala.toList)
    // generate new index
    val new_index = PythonManager.putNewVariable(nv)
    new_index
  }

  /*
  * UNION
  * */
  def union(index: Int, other: Int, left_name : String = "", right_name : String = ""): Int = {
    // get the corresponding variable
    val v = PythonManager.getVariable(index)
    val other_v = PythonManager.getVariable(other)

    val nv = v.UNION(other_v, left_name, right_name)
    // generate new index
    val new_index = PythonManager.putNewVariable(nv)
    new_index
  }

  /*
  * MERGE
  * */
  def merge(index: Int, groupBy : java.util.List[String]): Int = {
    // get the corresponding variable
    val v = PythonManager.getVariable(index)
    val groupByPar : Option[List[String]] = {
      if(groupBy.size() > 0)
        Option(groupBy.asScala.toList)
      else
        None
    }
    val nv = v.MERGE(groupByPar)
    // generate new index
    val new_index = PythonManager.putNewVariable(nv)
    new_index
  }

  /*
  * DIFFERENCE
  * */

  def difference(index: Int, other: Int, metaJoinCondition: Option[MetaJoinCondition],
                 is_exact: Boolean): Int = {
    val v = PythonManager.getVariable(index)
    val other_v = PythonManager.getVariable(other)

    val nv = v.DIFFERENCE(metaJoinCondition,other_v,is_exact)
    // generate new index
    val new_index = PythonManager.putNewVariable(nv)
    new_index
  }

}
