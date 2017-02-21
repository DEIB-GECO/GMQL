package it.polimi.genomics.compiler

import it.polimi.genomics.core.DataStructures.MetaJoinCondition.MetaJoinCondition
import it.polimi.genomics.core.DataStructures.RegionAggregate.RegionsToRegion

import scala.util.parsing.input.Position

/**
 * Created by pietro on 24/09/15.
 */
@SerialVersionUID(19L)
case class MapOperator(op_pos : Position,
                         input1 : Variable,
                         input2 : Option[Variable] = None,
                         output : VariableIdentifier,
                         parameters : OperatorParameters)
  extends Operator(op_pos,input1, input2, parameters)
  with BuildingOperator2 with Serializable {

  override val operator_name = "MAP"
  override val accepted_named_parameters = List("joinby", "count_name")
  var refined_agg_function_list : List[RegionsToRegion] = List.empty
  var meta_join_param : Option[MetaJoinCondition] = None
  var count_rename : Option[String] = None

  override def check_input_number = two_inputs

  override def preprocess_operator(status: CompilerStatus) : Boolean = {
    super_variable_left = Some(get_variable_if_defined(input1.asInstanceOf[VariableIdentifier].name, status).get.payload)
    super_variable_right = Some(get_variable_if_defined(input2.get.asInstanceOf[VariableIdentifier].name, status).get.payload)

    refined_agg_function_list =
      for (a <- parser_unnamed(map_aggfun_list, Some(List.empty)).get) yield {
        var fun : RegionsToRegion = null
        val new_field_name = if (a.output_field_name.isDefined) {
          Some(a.output_field_name.get.name)
        }else {
          None
        }
        val field_pos:Int = a.input_field_name match {
          case Some(FieldPosition(p)) => {right_var_check_num_field(p);p}
          case Some(FieldName(n)) => {right_var_get_field_name(n).get}
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
            val msg = "MAP operator at line " + op_pos.line + ": function name " + a.function_name +
              " is not available"
            throw new CompilerException(msg)
        }
        fun
    }
    val dup_fields_name = refined_agg_function_list
      .map(x=>x.output_name.get)
      .groupBy(identity)
      .collect { case (x,ys) if ys.size > 1 => x }

    for (d <- dup_fields_name) {
      val msg = "MAP operator at line " + op_pos.line + ": field name " + d + " " +
        "has multiple definitions."
      throw new CompilerException(msg)
    }

    for (n <- parameters.named) {
      n.param_name.trim.toLowerCase match {
        case "joinby" => {

          meta_join_param = Some(
            MetaJoinCondition(
              parser_named(
                metadata_attribute_list,
                n.param_name,
                n.param_value
              ).get))

        }
        case "count_name" => {

          val count_provided_name = parser_named(
            region_field_name,
            n.param_name,
            n.param_value)

          if (left_var_check_field_name_exists(count_provided_name.get)) {
            val msg = "'count_name' option of MAP operator at line " +
              op_pos.line + ": field name " + count_provided_name.get + " " +
              "already exists; cannot be reassigned."
            throw new CompilerException(msg)
          }

          count_rename = count_provided_name

        }
      }
    }

    true
  }

  def translate_operator(status : CompilerStatus):CompilerDefinedVariable = {

    val mapped = super_variable_left.get.MAP(
      meta_join_param,
      refined_agg_function_list,
      super_variable_right.get,
      Some(input1.name),
      Some(input2.get.name),
      count_rename
    )

    CompilerDefinedVariable(output.name,output.pos,mapped)
  }
}