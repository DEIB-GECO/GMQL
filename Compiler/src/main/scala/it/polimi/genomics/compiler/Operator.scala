package it.polimi.genomics.compiler

import it.polimi.genomics.core.DataStructures.CoverParameters.CoverParam
import it.polimi.genomics.core.DataStructures.GroupMDParameters.Direction.Direction
import it.polimi.genomics.core.DataStructures.GroupMDParameters.TopParameter
import it.polimi.genomics.core.DataStructures.IRVariable
import it.polimi.genomics.core.DataStructures.JoinParametersRD.AtomicCondition
import it.polimi.genomics.core.DataStructures.JoinParametersRD.RegionBuilder.RegionBuilder
import it.polimi.genomics.core.DataStructures.MetaGroupByCondition.MetaGroupByCondition
import it.polimi.genomics.core.DataStructures.RegionAggregate.RENode
import it.polimi.genomics.core.DataStructures.RegionCondition.REG_OP._
import it.polimi.genomics.core.DataStructures.RegionCondition.RegionCondition
import org.slf4j.LoggerFactory

import scala.util.parsing.input.{CharSequenceReader, Positional, Position}

/**
 * Created by pietro on 12/09/15.
 */

abstract class Operator (op_pos : Position,
                             input1 : Variable,
                             input2 : Option[Variable] = None,
                             parameters : OperatorParameters) extends GmqlParsers {

  final val logger = LoggerFactory.getLogger(this.getClass)
  val operator_name : String
  val accepted_named_parameters : List[String]
  var super_variable_left : Option[IRVariable] = None
  var super_variable_right : Option[IRVariable] = None

  val unsupported_default_parameter = "Operator " + operator_name + " at line " + op_pos.line +
    " does not support default parameters"


  @throws[CompilerException]
  def check_input_number() : Boolean


  def one_input() : Boolean = {
    if (input2.isDefined) {
      val msg : String = "Operator " + operator_name + " accepts only one input; " +
                         "two provided (" + input1.name + ", " + input2.get.name + ")"
      throw new CompilerException(msg)
    }
    true
  }

  def two_inputs() : Boolean = {
    if (!input2.isDefined) {
      val msg : String = "Operator " + operator_name + " requires two input variables; " +
        "only one provided (" + input1.name + ")"
      throw new CompilerException(msg)

    }
    true
  }

  def check_input_variables(status : CompilerStatus) : Boolean = {
    input1 match {
      case v:VariableIdentifier => {if(!status.getVariable(v.name).isDefined){
        val msg : String = "Variable " + v.name + " at line " + v.pos.line + " has no previous definition"
        throw new CompilerException(msg)
      }}
      case _ =>
    }


    if(input2.isDefined) {
      input2 match {
        case v:VariableIdentifier => {if(!status.getVariable(v.name).isDefined){
          val msg : String = "Variable " + v.name + " at line " + v.pos.line + " has no previous definition"
          throw new CompilerException(msg)
        }}
        case _ =>
      }
    }
    true
  }


  def check_named_parameters() : Boolean = {
    for (x <- parameters.named) {
      if (!accepted_named_parameters.contains(x.param_name.toLowerCase)) {
        val msg = "Operator " + operator_name + " at line " + op_pos.line + " does not accept parameter \"" + x.param_name + "\". " +
          "Available options are: " + accepted_named_parameters.mkString(", ")
        throw new CompilerException(msg)
      }
    }
    true
  }

  def parser_unnamed[T](parser : Parser[T], default : Option[T]) : Option[T] = {
    if (parameters.unamed.isDefined) {
      val msg = "Operator " + operator_name + " at line " + op_pos.line + ": invalid unnamed parameter " + parameters.unamed.get
        parse(parser, new CharSequenceReader(parameters.unamed.get.trim)) match {
          case Success(r,n) => {
            if (n.atEnd) {
              Some(r)
            }
            else {
              throw new CompilerException(msg)
              None
            }
          }
          case _ => {
            throw new CompilerException(msg)
            None
          }
        }
      }
      else {
        default
      }
  }

  def parser_named[T](parser : Parser[T], name : String, value: String, additional_error_info : Option[String] = None) : Option[T] = {
    val msg = "Operator " + operator_name + " at line " + op_pos.line + ": invalid syntax for parameter '" +
      name + "'. String '" + value + "' not recognized. " + additional_error_info.getOrElse("")
    parse(parser, new CharSequenceReader(value.trim)) match {
      case Success(r,n) =>
        if (n.atEnd) {
          Some(r)
        } else {
          throw new CompilerException(msg)
          None
        }
      case _ => {
        throw new CompilerException(msg)
        None
      }
    }
  }


  def left_var_check_num_field(pos : Int) : Boolean = {
    if(pos < super_variable_left.get.get_number_of_fields){
      true
    }else{
      val msg = "In " + operator_name + " at line " + op_pos.line + " field at position " +
        pos + " does not exist"
      throw new CompilerException(msg)
      false
    }
  }

  def right_var_check_num_field(pos : Int) : Boolean = {
    if(pos < super_variable_right.get.get_number_of_fields){
      true
    }else{
      val msg = "In " + operator_name + " at line " + op_pos.line + " field at position " +
        pos + " does not exist"
      throw new CompilerException(msg)
      false
    }
  }

  def left_var_get_field_name(name : String) : Option[Int] = {
    val fp = super_variable_left.get.get_field_by_name(name)
    if (fp.isDefined) {
      fp
    }
    else {
      val schema_list = super_variable_left.get.schema.map(_._1).mkString(", ")
      val msg = "In " + operator_name + " at line " + op_pos.line + " field " + name +
        " is not defined. Avalilable fields are { " + schema_list + " } "
      throw new CompilerException(msg)
      None
    }
  }

  def left_var_get_field_name_wildcards(name : String) : List[Int] = {

    super_variable_left.get.get_field_by_name_with_wildcard(name)

  }

  def right_var_get_field_name(name : String) : Option[Int] = {
    val fp = super_variable_right.get.get_field_by_name(name)
    if (fp.isDefined) {
      fp
    }
    else {
      val schema_list = super_variable_right.get.schema.map(_._1).mkString(", ")
      val msg = "In " + operator_name + " at line " + op_pos.line + " field " + name +
        " is not defined. Avalilable fields are { " + schema_list + " } "
      throw new CompilerException(msg)
      None
    }
  }

  def make_sure_undefined(name : String): Unit = {
    if (super_variable_left.get.get_field_by_name(name).isDefined) {
      val msg = "In " + operator_name + " at line " + op_pos.line + " field " + name +
      " already has a defintion and cannot be redefined."
      throw new CompilerException(msg)
    }
  }

  def get_variable_if_defined(name:String, status:CompilerStatus) : Option[CompilerDefinedVariable] = {
    val response = status.getVariable(name)
    if (!response.isDefined) {
      val msg = "In " + operator_name + " variable " + name + " is undefined."
      throw new CompilerException(msg)
    }
    response
  }


}

trait BuildingOperator2 {
  def output : VariableIdentifier

  @throws[CompilerException]
  def check_output_variable(status : CompilerStatus) : Boolean = {
    if(status.getVariable(output.name).isDefined) {
      val predef:CompilerDefinedVariable = status.getVariable(output.name).get
      val msg : String = "Redefinition of variable " + output.name + " at line " +
        output.pos.line + " not allowed. Previous definition at line " +
        predef.pos.line + "\n"
      throw new CompilerException(msg)
    }
    true
  }

  @throws[CompilerException]
  def preprocess_operator(status: CompilerStatus) : Boolean

  def translate_operator(status : CompilerStatus):CompilerDefinedVariable

}


trait FieldPositionOrName
case class FieldPosition(pos : Int) extends FieldPositionOrName
case class FieldName(name : String) extends FieldPositionOrName
case class FieldNameWithWildCards(name_with_wildcards : String) extends FieldPositionOrName

case class MetaJoinConditionTemp(attributes : List[String], join_dataset_name : VariableIdentifier)
case class RegionPredicateTemp(field:String, operator : REG_OP,  value : Any)  extends RegionCondition {}


trait SingleProjectOnMeta
case class MetaProject(attribute_name : String) extends  SingleProjectOnMeta
case class MetaModifier() extends SingleProjectOnMeta

trait SingleProjectOnRegion
case class RegionProject(field: FieldPositionOrName) extends SingleProjectOnRegion
case class REFieldNameOrPosition(field: FieldPositionOrName) extends RENode
case class RegionModifier(field: FieldPositionOrName, dag : RENode) extends SingleProjectOnRegion
case class AllBut(fields: List[FieldPositionOrName])

trait CoverType extends Positional
case class Cover() extends CoverType {override def toString() = "COVER"}
case class Histogram() extends CoverType {override def toString() = "HISTOGRAM"}
case class Flat() extends CoverType {override def toString() = "FLAT"}
case class Summit() extends CoverType {override def toString() = "SUMMIT"}

case class RegionsToRegionTemp(function_name : String,
                               input_field_name : Option[FieldPositionOrName],
                               output_field_name : Option[FieldName])

case class RegionsToMetaTemp(function_name : String,
                              input_field_name : Option[FieldPositionOrName],
                              output_attribute_name : Option[String])

case class MapParameters(meta_join_condition : Option[List[String]],
                         aggs: List[RegionsToRegionTemp])

case class JoinParameters(meta_join_condition : Option[List[String]],
                           region_condition : List[AtomicCondition],
                           region_builder : RegionBuilder)

case class CoverParameters(meta_group_condition : Option[List[String]],
                            min_accumulation : CoverParam,
                            max_accumulation : CoverParam,
                            aggs: Option[List[RegionsToRegionTemp]])

case class DifferenceParameters(meta_join_condition : Option[List[String]])

case class MergeParameters(meta_join_condition : Option[List[String]])

case class ExtendParameters(aggs : List[RegionsToMetaTemp])

case class MetaOrderParameters(ordering : List[(String, Direction)], top : TopParameter)

case class RegionOrderParameters(ordering : List[(FieldPositionOrName, Direction)], top : TopParameter)

case class OrderParameters(meta_order : Option[MetaOrderParameters], region_order : Option[RegionOrderParameters])

case class GroupMetaParameters(grouping : Option[MetaGroupByCondition], aggregates : Option[List[RegionsToMetaTemp]])

case class GroupRegionParameters(grouping : Option[List[FieldPositionOrName]], aggregates : Option[List[RegionsToRegionTemp]])

case class GroupParameters(meta_grouping : Option[GroupMetaParameters], region_grouping : Option[GroupRegionParameters])


case class NamedParameter(param_name : String, param_value : String)

case class OperatorParameters(unamed : Option[String], named : List[NamedParameter])


