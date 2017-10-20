package it.polimi.genomics.compiler

import it.polimi.genomics.core.DataStructures.GroupRDParameters.{FIELD, GroupingParameter}
import it.polimi.genomics.core.DataStructures.MetaAggregate.MetaAggregateFunction
import it.polimi.genomics.core.DataStructures.MetaGroupByCondition.MetaGroupByCondition
import it.polimi.genomics.core.DataStructures.MetaJoinCondition.Default
import it.polimi.genomics.core.DataStructures.RegionAggregate.{RegionsToMeta, RegionsToRegion}

import scala.util.parsing.input.Position

/**
 * Created by pietro on 28/09/15.
 */

@SerialVersionUID(28L)
case class GroupOperator(op_pos : Position,
                           input1 : Variable,
                           input2 : Option[Variable] = None,
                           output : VariableIdentifier,
                           parameters : OperatorParameters)
  extends Operator(op_pos,input1, input2, parameters)
    with BuildingOperator2 with Serializable {


  override val operator_name = "GROUP"
  override val accepted_named_parameters = List(
    "meta_aggregates",
    "meta_group_name",
    "region_keys",
    "region_aggregates")

  override def check_input_number = one_input

  var meta_keys:Option[MetaGroupByCondition] = None
  var refined_meta_aggregate_function_list: Option[List[MetaAggregateFunction]] = None
  var meta_group_name:Option[String] = None
  var refined_region_aggregate_function_list : Option[List[RegionsToRegion]] = None
  var region_keys : Option[List[GroupingParameter]] = None




  @throws[CompilerException]
  def preprocess_operator(status: CompilerStatus) : Boolean = {

    super_variable_left = Some(
      get_variable_if_defined(input1.asInstanceOf[VariableIdentifier].name,
        status).get.payload
    )

    if (parameters.unamed.isDefined) {
      meta_keys = Some (
        MetaGroupByCondition(
          parser_unnamed(metadata_attribute_list, None).get.map(Default(_)))
      )
    }

    for (p <- parameters.named) {
      p.param_name.trim.toLowerCase() match {

        case "meta_aggregates" => {
          refined_meta_aggregate_function_list = Some(
            for (a <- parser_named(group_meta_aggfun_list, p.param_name.trim, p.param_value.trim).get) yield {

              try {

                status.get_server.implementation.metaAggregateFunctionFactory.get(a.fun_name,a.input,a.output)

              } catch {
                case e:Exception =>
                  val msg = "At operator " + operator_name + " at line " + op_pos.line +
                    " : " + e.getMessage
                  throw new CompilerException(msg)
              }
            }
          )
        }
        case "meta_group_name" => {
          meta_group_name = parser_named(metadata_attribute, p.param_name.trim, p.param_value.trim)
        }

        case "region_keys" => {
          //region_keys : Option[List[GroupingParameter]] = None
          region_keys =
            Some(
              parser_named(group_region_keys, p.param_name.trim, p.param_value.trim)
                .get
                .map(_.name)
                .toSet
                .toList
                .map((x:String) => left_var_get_field_name(x).get)
                .map(FIELD)
            )
        }

        case "region_aggregates" => {

          refined_region_aggregate_function_list = Some(
            for (a <- parser_named(map_aggfun_list, p.param_name.trim, p.param_value.trim).get) yield {
              var fun : RegionsToRegion = null
              val new_field_name = if (a.output_field_name.isDefined) {
                Some(a.output_field_name.get.name)
              }else {
                None
              }
              val field_pos:Int = a.input_field_name match {
                case Some(FieldPosition(p)) => {left_var_check_num_field(p);p}
                case Some(FieldName(n)) => {left_var_get_field_name(n).get}
                case None => 0
              }
              if(new_field_name.isDefined){
                make_sure_undefined(new_field_name.get)
              }

              try {
                fun = a.input_field_name match {
                  case Some(_) =>  status.get_server.implementation
                    .mapFunctionFactory.get(a.function_name,field_pos,new_field_name)
                  case None => status.get_server.implementation
                    .mapFunctionFactory.get(a.function_name,new_field_name)}
                fun.function_identifier = a.function_name
                fun.input_index = field_pos
                fun.output_name = new_field_name
              } catch {
                case e:Exception =>
                  val msg = operator_name + " operator at line " + op_pos.line + ": function name "+
                    a.function_name +
                    " is not available"
                  throw new CompilerException(msg)
              }
              fun
            }
          )

        }

      }
    }
    true
  }

  override def translate_operator(status : CompilerStatus):CompilerDefinedVariable = {


    //check parameters validity

    if (!meta_keys.isDefined && refined_meta_aggregate_function_list.isDefined) {
      val msg = operator_name + " operator at line " + op_pos.line + ": " +
        "if metadata aggregate functions are provided, then metadata keys are required."
      throw new CompilerException(msg)
    }

    if (!region_keys.isDefined && refined_region_aggregate_function_list.isDefined) {
      val msg = operator_name + " operator at line " + op_pos.line + ": " +
        "if region aggregate functions are provided, then region keys are required."
      throw new CompilerException(msg)
    }


    val mapped = super_variable_left.get.GROUP(
      meta_keys,
      refined_meta_aggregate_function_list,
      meta_group_name.getOrElse("_group"),
      region_keys,
      refined_region_aggregate_function_list
    )
    CompilerDefinedVariable(output.name,output.pos,mapped)
  }

}
