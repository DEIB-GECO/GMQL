package it.polimi.genomics.compiler

import it.polimi.genomics.core.DataStructures.MetadataCondition.MetadataCondition
import it.polimi.genomics.core.DataStructures.RegionAggregate.RegionsToMeta
import it.polimi.genomics.core.DataStructures.RegionCondition.RegionCondition

import scala.util.parsing.input.Position

/**
 * Created by pietro on 28/09/15.
 */
@SerialVersionUID(29L)
case class ExtendOperator(op_pos : Position,
                          input1 : Variable,
                          input2 : Option[Variable] = None,
                          output : VariableIdentifier,
                          parameters : OperatorParameters)
  extends Operator(op_pos,input1, input2, parameters)
  with BuildingOperator2 with Serializable {

  override def check_input_number = one_input
  override val operator_name = "EXTEND"
  override val accepted_named_parameters = List.empty
  var refined_aggregate_function_list: List[RegionsToMeta] = List.empty

  override def preprocess_operator(status: CompilerStatus) : Boolean = {

    super_variable_left = Some(get_variable_if_defined(input1.asInstanceOf[VariableIdentifier].name, status).get.payload)

    refined_aggregate_function_list = for (a <- parser_unnamed(extend_aggfun_list, Option(List.empty)).get) yield {
      val position:Option[Int] = a.input_field_name match {
        case Some(FieldPosition(p)) => {left_var_check_num_field(p); Some(p)}
        case Some(FieldName(n)) => {left_var_get_field_name(n)}
        case None => None
        }
      var fun : RegionsToMeta = null
      try {
        fun = if (position.isDefined) {
          status.get_server.implementation.extendFunctionFactory.get(a.function_name,position.get,a.output_attribute_name)
        } else {
          status.get_server.implementation.extendFunctionFactory.get(a.function_name,a.output_attribute_name)
        }
        if (position.isDefined) {
          fun.input_index = position.get
        }
        fun.function_identifier = a.function_name
        fun.output_attribute_name = a.output_attribute_name.getOrElse("default_aggregate")
      } catch {
        case e:Exception =>
          val msg = "At operator " + operator_name + " at line " + op_pos.line +
            " : " + e.getMessage
          throw new CompilerException(msg)
      }
      fun
    }

    if (refined_aggregate_function_list.isEmpty) {
      val msg = "At operator " + operator_name + " at line " + op_pos.line +
        " : empty parameter list is not allowed. Please specify some."
      throw new CompilerException(msg)

    }
    true
  }

  def translate_operator(status : CompilerStatus):CompilerDefinedVariable = {

    val extended = super_variable_left.get.EXTEND(refined_aggregate_function_list)

    CompilerDefinedVariable(output.name,output.pos,extended)
  }

}