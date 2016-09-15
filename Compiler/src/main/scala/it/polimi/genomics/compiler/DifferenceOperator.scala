package it.polimi.genomics.compiler

import it.polimi.genomics.core.DataStructures.MetaJoinCondition.MetaJoinCondition

import scala.util.parsing.input.Position

/**
 * Created by pietro on 27/09/15.
 */
@SerialVersionUID(30L)
case class DifferenceOperator(op_pos : Position,
                        input1 : Variable,
                        input2 : Option[Variable] = None,
                        output : VariableIdentifier,
                        parameters : OperatorParameters)
  extends Operator(op_pos,input1, input2, parameters)
  with BuildingOperator2 with Serializable {

  override val operator_name = "DIFFERENCE"
  override val accepted_named_parameters = List("joinby")
  var meta_join_param : Option[MetaJoinCondition] = None

  override def check_input_number = two_inputs

  override def preprocess_operator(status: CompilerStatus) : Boolean = {
    super_variable_left = Some(get_variable_if_defined(input1.asInstanceOf[VariableIdentifier].name, status).get.payload)
    super_variable_right = Some(get_variable_if_defined(input2.get.asInstanceOf[VariableIdentifier].name, status).get.payload)

    if (parameters.unamed.isDefined) {
      throw new CompilerException(unsupported_default_parameter)
    }

    for (n <- parameters.named) {
      n.param_name.trim.toLowerCase match {
        case "joinby" => {
          meta_join_param = Some(MetaJoinCondition(parser_named(metadata_attribute_list, n.param_name, n.param_value).get))
        }
      }
    }

    true
  }

  def translate_operator(status : CompilerStatus):CompilerDefinedVariable = {

    val differenced = super_variable_left.get.DIFFERENCE(
      meta_join_param,
      super_variable_right.get)

    CompilerDefinedVariable(output.name,output.pos,differenced)
  }
}
