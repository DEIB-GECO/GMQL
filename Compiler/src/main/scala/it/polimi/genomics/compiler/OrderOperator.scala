package it.polimi.genomics.compiler

import it.polimi.genomics.core.DataStructures.GroupMDParameters.Direction.Direction
import it.polimi.genomics.core.DataStructures.GroupMDParameters._
import it.polimi.genomics.core.DataStructures.MetadataCondition.MetadataCondition
import it.polimi.genomics.core.DataStructures.RegionCondition.RegionCondition

import scala.util.parsing.input.Position

/**
 * Created by pietro on 28/09/15.
 */

@SerialVersionUID(17L)
case class OrderOperator(op_pos : Position,
                          input1 : Variable,
                          input2 : Option[Variable] = None,
                          output : VariableIdentifier,
                          parameters : OperatorParameters)
  extends Operator(op_pos,input1, input2, parameters)
  with BuildingOperator2 with Serializable {

  override val operator_name = "ORDER"
  override val accepted_named_parameters = List(
    "meta_top",
    "meta_topg",
    "meta_topp",
    "region_order",
    "region_top",
    "region_topg",
    "region_topp")

  var meta_order:Option[List[(String,Direction)]] = None
  var meta_top:TopParameter = NoTop()

  var region_order:Option[List[(FieldPositionOrName,Direction)]] = None
  var region_top:TopParameter = NoTop()
  var refined_region_ordering : Option[List[(Int,Direction)]] = None

  override def check_input_number = one_input

  override def preprocess_operator(status: CompilerStatus) : Boolean = {
    super_variable_left = Some(get_variable_if_defined(input1.asInstanceOf[VariableIdentifier].name, status).get.payload)

    if (parameters.unamed.isDefined){
      meta_order = parser_unnamed(meta_order_list, None)
    }
    for (n <- parameters.named) {
      n.param_name.trim.toLowerCase match {
        case "meta_top" => { meta_top = Top(parser_named(wholeNumber,n.param_name,n.param_value).get.toInt)}
        case "meta_topg" => { meta_top = TopG(parser_named(wholeNumber,n.param_name,n.param_value).get.toInt)}
        case "meta_topp" => {meta_top = TopP(parser_named(wholeNumber,n.param_name,n.param_value).get.toInt)}
        case "region_order" => { region_order = parser_named(region_order_list, n.param_name,n.param_value)}
        case "region_top" => { region_top = Top(parser_named(wholeNumber,n.param_name,n.param_value).get.toInt)}
        case "region_topg" => { region_top = TopG(parser_named(wholeNumber,n.param_name,n.param_value).get.toInt)}
        case "region_topp" => {region_top = TopP(parser_named(wholeNumber,n.param_name,n.param_value).get.toInt)}
      }
    }

    if (region_order.isDefined){
      refined_region_ordering = Some(for(o <- region_order.get) yield {
        o match {
          case (FieldName(n),d) => {
            (left_var_get_field_name(n).get,d)
            }
          case (FieldPosition(p),d) => {
            left_var_check_num_field(p)
            (p,d)
          }
        }
      })
    }
    true
  }

  override def translate_operator(status : CompilerStatus):CompilerDefinedVariable = {

    val sorted = super_variable_left.get.ORDER(
      meta_order,
      "_order",
      meta_top,
      refined_region_ordering,
      region_top)

    CompilerDefinedVariable(output.name,output.pos,sorted)
  }

}