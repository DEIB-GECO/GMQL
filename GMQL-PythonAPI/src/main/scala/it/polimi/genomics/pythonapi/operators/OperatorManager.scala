package it.polimi.genomics.pythonapi.operators

import it.polimi.genomics.core.DataStructures.CoverParameters._
import it.polimi.genomics.core.DataStructures.CoverParameters.CoverFlag.CoverFlag
import it.polimi.genomics.core.DataStructures.GroupMDParameters.Direction.{ASC, DESC, Direction}
import it.polimi.genomics.core.DataStructures.GroupMDParameters._
import it.polimi.genomics.core.DataStructures.GroupRDParameters.{FIELD, GroupingParameter}
import it.polimi.genomics.core.DataStructures.{IRVariable, MetaOperator}
import it.polimi.genomics.core.DataStructures.JoinParametersRD._
import it.polimi.genomics.core.DataStructures.MetaAggregate.{MetaAggregateFunction, MetaAggregateStruct, MetaExtension}
import it.polimi.genomics.core.DataStructures.JoinParametersRD.RegionBuilder.RegionBuilder
import it.polimi.genomics.core.DataStructures.MetaGroupByCondition.MetaGroupByCondition
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

  @deprecated
  def meta_select(index: Int, other: Int, metaCondition: MetadataCondition): Int = {
    meta_select(index, other, metaCondition, None)
  }

  @deprecated
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

  @deprecated
  def reg_select(index: Int, other: Int, regionCondition: RegionCondition): Int = {
    reg_select(index, other, regionCondition, None)
  }

  @deprecated
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

  @deprecated
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

  def select(index: Int, other:Option[Int], semi_join_condition : Option[MetaJoinCondition],
             meta_condition : Option[MetadataCondition], region_condition : Option[RegionCondition]): Int = {
    val v = PythonManager.getVariable(index)
    var ov: Option[MetaOperator] = None
    if(other.isDefined)
      ov = Some(PythonManager.getVariable(other.get).metaDag)
    val nv = v.add_select_statement(ov, semi_join_condition, meta_condition, region_condition)
    // generate new index
    val new_index = PythonManager.putNewVariable(nv)
    new_index
  }

  /*
  * PROJECT
  * */

  def project(index: Int, projected_meta : Option[java.util.List[String]],
              extended_meta : Option[java.util.List[MetaExtension]],
              all_but_meta: Boolean,
              projected_regs : Option[java.util.List[String]],
              all_but_regs : Option[java.util.List[String]],
              extended_regs : Option[java.util.List[RegionFunction]]
             ): Int = {

    val v = PythonManager.getVariable(index)

    // PROJECTED META
    var projected_meta_scala: Option[List[String]] = None
    if(projected_meta.isDefined){
      //println("Projected Meta is defined")
      projected_meta_scala = Some(projected_meta.get.asScala.toList)
    }
    // ALL BUT
    var all_but_scala: Option[List[String]] = None
    if(all_but_regs.isDefined){
      //println("All but is defined")
      all_but_scala = Some(all_but_regs.get.asScala.toList)
    }

    // PROJECTED REGIONS
    var projected_regs_scala: Option[List[Int]] = None
    if(projected_regs.isDefined){
      //println("Projected regs is defined")
      projected_regs_scala = Some(projected_regs.get.asScala.toList.map( x => {
        val pos = v.get_field_by_name(x)
        if(pos.isDefined){
          pos.get
        }
        else
          throw new IllegalArgumentException("The attribute " + x + " is not present")
      }))
      //println(projected_regs_scala.get.toString())
    }

    // EXTENDED REGIONS
    var extended_regs_scala: Option[List[RegionFunction]] = None
    if(extended_regs.isDefined){
      //println("Extended regs is defined")
      extended_regs_scala = Some(extended_regs.get.asScala.toList)
      //extended_regs_scala.get.map(x => println(x.inputIndexes + "\t" + x.output_index + "\t" + x.output_name))
    }

    // EXTENDED META
    var extended_meta_scala : Option[List[MetaExtension]] = None
    if(extended_meta.isDefined) {
      extended_meta_scala = Some(extended_meta.get.asScala.toList)
    }

    val nv = v.PROJECT(projected_meta_scala, extended_meta_scala, all_but_meta,
      projected_regs_scala,all_but_scala, extended_regs_scala)

    val new_index = PythonManager.putNewVariable(nv)
    new_index
  }

  /*
  * COVER
  * */

  def getCoverTypes(name : String): CoverFlag = {
    name.toLowerCase match {
      case "normal" => CoverFlag.COVER
      case "flat" => CoverFlag.FLAT
      case "summit" => CoverFlag.SUMMIT
      case "histogram" => CoverFlag.HISTOGRAM
      case _ => throw new IllegalArgumentException(name + " is not a valid COVER type")
    }
  }

  def getCoverParam(p : String): CoverParam = {
    p.toUpperCase match {
      case "ALL" => new ALL{}
      case "ANY" => new ANY{}
      case _ =>
        try {
          val number = p.toInt
          new N {
            override val n: Int = number
          }
        } catch {
          case _ : Exception => throw new IllegalArgumentException(p + " is an invalid COVER parameter")
        }
    }
  }

  def cover(index: Int, coverFlag : CoverFlag, minAcc : CoverParam, maxAcc : CoverParam,
            groupBy : Option[java.util.List[String]], aggregates: java.util.List[RegionsToRegion]): Int = {
    val aggregatesPar = aggregates.asScala.toList
    val groupByCondition = {
      if(groupBy.isDefined)
        Some(getAttributeEvaluationStrategy(groupBy.get.asScala.toList))
      else
        None
    }
    val v = PythonManager.getVariable(index)
    val nv = v COVER(coverFlag,minAcc,maxAcc,aggregatesPar,groupByCondition)
    // generate new index
    val new_index = PythonManager.putNewVariable(nv)
    new_index
  }

  def getGenometricCondition(conditionName : String, argument : String): AtomicCondition = {
    conditionName match {
      case "DLE" =>
        logger.info("DLE("+argument.toLong+")")
        DistLess(argument.toLong + 1)
      case "DGE" =>
        logger.info("DGE("+argument.toLong+")")
        DistGreater(argument.toLong - 1)
      case "DL" => DistLess(argument.toLong)
      case "DG" => DistGreater(argument.toLong)
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

  def getMetaJoinCondition(metadataAttributeList : java.util.List[String]): MetaJoinCondition = {
    // TODO: enable different evaluation strategies
    var listAttributes = new ListBuffer[AttributeEvaluationStrategy]()
    for(m <- metadataAttributeList) {
      val att = Default(m)
      listAttributes += att
    }
    MetaJoinCondition(listAttributes.toList)
  }

  def getAttributeEvaluationStrategy(metadataAttributeList : List[String]): List[AttributeEvaluationStrategy] = {
    // TODO: enable different evaluation strategies
    metadataAttributeList.map(x => Default(x))
  }

  def getRegionBuilderJoin(builder: String) : RegionBuilder = {
    builder.toUpperCase match {
      case "LEFT" =>  RegionBuilder.LEFT
      case "LEFT_DISTINCT" => RegionBuilder.LEFT_DISTINCT
      case "RIGHT" => RegionBuilder.RIGHT
      case "RIGHT" =>  RegionBuilder.RIGHT_DISTINCT
      case "INTERSECTION" => RegionBuilder.INTERSECTION
      case "CONTIG" => RegionBuilder.CONTIG
      case "BOTH" => RegionBuilder.BOTH
      case _ => throw new IllegalArgumentException(builder + " is not a region builder")
    }
  }

  def join(index: Int, other: Int, metaJoinCondition: Option[MetaJoinCondition],
           regionJoinCondition: List[JoinQuadruple], regionBuilder : RegionBuilder,
           referenceName: String, experimentName: String,
           left_on : Option[java.util.List[String]], right_on : Option[java.util.List[String]]): Int = {

    var refName : Option[String] = None
    var expName : Option[String] = None

    if(referenceName.length() > 0)
      refName = Option(referenceName)

    if(experimentName.length() > 0)
      expName = Option(experimentName)

    val v = PythonManager.getVariable(index)
    val other_v = PythonManager.getVariable(other)

    val join_on_attributes : Option[List[(Int, Int)]] = {
      if(left_on.isDefined && right_on.isDefined){
        val left_idxs = left_on.get.toList.map(x => {
          val r = v.get_field_by_name(x)
          if(r.isDefined)
            r.get
          else
            throw new IllegalArgumentException("Region field " + x + " is not present")
        })
        val right_idxs = right_on.get.toList.map(x => {
          val r = other_v.get_field_by_name(x)
          if(r.isDefined)
            r.get
          else
            throw new IllegalArgumentException("Region field " + x + " is not present")
        })
        Some(left_idxs.zip(right_idxs))
      }
      else
        None
    }

    val nv = v.JOIN(metaJoinCondition,regionJoinCondition,regionBuilder,other_v,refName,expName, join_on_attributes)
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

  def getOrderTopParameter(paramName : String, k : Int) : TopParameter = {
    paramName.toUpperCase match {
      case "TOP" => Top(k)
      case "TOPG" => TopG(k)
      case "TOPP" => TopP(k)
      case "NOTOP" => NoTop()
      case _ => throw new IllegalArgumentException(paramName + " is not an ordering parameter")
    }
  }

  def getOrderDirection(value: Boolean): Direction = {
    if(value){
      ASC
    }
    else
      DESC
  }

  def order(index: Int, metaOrdering: Option[java.util.List[String]],
            metaAscending: Option[java.util.List[Boolean]],
            metaTop: Option[String], metaK: Option[Int],
            regionOrdering: Option[java.util.List[String]],
            regionAscending: Option[java.util.List[Boolean]],
            regionTop: Option[String], regionK: Option[Int]) : Int = {

    val v = PythonManager.getVariable(index)
    val metaTopParameter = {
      if(metaTop.isDefined &&  metaK.isDefined)
        getOrderTopParameter(metaTop.get, metaK.get)
      else
        getOrderTopParameter("NOTOP", 0)
    }

    val regionTopParameter = {
      if(regionTop.isDefined &&  regionK.isDefined)
        getOrderTopParameter(regionTop.get, regionK.get)
      else
        getOrderTopParameter("NOTOP", 0)
    }

    val metaOrderingP = {
      if(metaOrdering.isDefined && metaAscending.isDefined){
        Some(metaOrdering.get.toList.zip(metaAscending.get.toList.map(getOrderDirection)))
      }
      else
        None
    }

    val regionOrderingP = {
      if(regionOrdering.isDefined && regionAscending.isDefined){
        Some(regionOrdering.get.toList.map(x => {
          val r = v.get_field_by_name(x)
          if(r.isDefined)
            r.get
          else
            throw new IllegalArgumentException(x + " is not a valid region field")
        }).zip(regionAscending.get.toList.map(getOrderDirection)))
      }
      else
        None
    }
    val nv = v.ORDER(metaOrderingP,"_group",metaTopParameter,regionOrderingP,regionTopParameter)
    // generate new index
    val new_index = PythonManager.putNewVariable(nv)
    new_index
  }

  /*
  * GROUP
  * */

  def getMetaGroupByCondition(meta_keys: List[String]): MetaGroupByCondition = {
    MetaGroupByCondition(getAttributeEvaluationStrategy(meta_keys))
  }

  def getGroupingParameters(region_keys: List[String], v: IRVariable): List[GroupingParameter] = {
    region_keys.map(x => {
      val fieldNum = v.get_field_by_name(x)
      if(fieldNum.isEmpty)
        throw new IllegalArgumentException("Region attribute " + x + " is not present!")
      else
        FIELD(fieldNum.get)
    })
  }

  def group(index: Int, meta_keys : Option[java.util.List[String]],
            meta_aggregates : Option[java.util.List[MetaAggregateFunction]],
            meta_group_name : String,
            region_keys: Option[java.util.List[String]],
            region_aggregates : Option[java.util.List[RegionsToRegion]]) : Int = {
    val v = PythonManager.getVariable(index)
    val metaKeysP = {
      if(meta_keys.isDefined)
        Some(getMetaGroupByCondition(meta_keys.get.toList))
      else
        None
    }

    val metaAggregates = {
      if(meta_aggregates.isDefined)
        Some(meta_aggregates.get.toList)
      else
        None
    }

    val regionKeysP = {
      if(region_keys.isDefined)
        Some(getGroupingParameters(region_keys.get.toList, v))
      else
        None
    }

    val regionAggregates = {
      if(region_aggregates.isDefined)
        Some(region_aggregates.get.toList)
      else
        None
    }

    val nv = v GROUP(metaKeysP, metaAggregates, meta_group_name, regionKeysP, regionAggregates)
    val new_index = PythonManager.putNewVariable(nv)
    new_index
  }


//  def group(index: Int, meta_keys: Option[java.util.List[String]],
//            meta_aggregates: Option[java.util.List[RegionsToMeta]], meta_group_name: String,
//            region_keys: Option[java.util.List[String]],
//            region_aggregates: Option[java.util.List[RegionsToRegion]]): Int = {
//
//    val v = PythonManager.getVariable(index)
//
//    var meta_keys_condition: Option[MetaGroupByCondition] = None
//    if(meta_keys.isDefined) {
//      meta_keys_condition = Some(getMetaGroupByCondition(meta_keys.get.asScala.toList))
//    }
//
//    var meta_aggregates_scala: Option[List[RegionsToMeta]] = None
//    if(meta_aggregates.isDefined) {
//      meta_aggregates_scala = Some(meta_aggregates.get.asScala.toList)
//    }
//
//    var region_keys_condition: Option[List[GroupingParameter]] = None
//    if(region_keys.isDefined) {
//      region_keys_condition = Some(getGroupingParameters(region_keys.get.asScala.toList, v))
//    }
//
//    var region_aggregates_scala: Option[List[RegionsToRegion]] = None
//    if(region_aggregates.isDefined) {
//      region_aggregates_scala = Some(region_aggregates.get.asScala.toList)
//    }
//
//    val nv = v GROUP(meta_keys_condition, meta_aggregates_scala, meta_group_name,
//      region_keys_condition, region_aggregates_scala)
//
//    val new_index = PythonManager.putNewVariable(nv)
//    new_index
//  }

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
  def merge(index: Int, groupBy : Option[java.util.List[String]]): Int = {
    // get the corresponding variable
    val v = PythonManager.getVariable(index)
    val groupByCondition = {
      if(groupBy.isDefined)
        Some(getAttributeEvaluationStrategy(groupBy.get.asScala.toList))
      else
        None
    }
    val nv = v.MERGE(groupByCondition)
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
