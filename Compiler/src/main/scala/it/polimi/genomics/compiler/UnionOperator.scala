package it.polimi.genomics.compiler

import it.polimi.genomics.core.DataStructures.MetaJoinCondition.MetaJoinCondition

import scala.util.parsing.input.Position

/**
 * Created by pietro on 28/09/15.
 */
@SerialVersionUID(23L)
case class UnionOperator(op_pos : Position,
                               input1 : Variable,
                               input2 : Option[Variable] = None,
                               output : VariableIdentifier,
                               parameters : OperatorParameters)
  extends Operator(op_pos,input1, input2, parameters)
  with BuildingOperator2 with Serializable {

  override val operator_name = "UNION"
  override val accepted_named_parameters = List("")
  var meta_join_param : Option[MetaJoinCondition] = None

  override def check_input_number = two_inputs

  override def preprocess_operator(status: CompilerStatus) : Boolean = {
    super_variable_left = Some(get_variable_if_defined(input1.asInstanceOf[VariableIdentifier].name, status).get.payload)
    super_variable_right = Some(get_variable_if_defined(input2.get.asInstanceOf[VariableIdentifier].name, status).get.payload)

    if (parameters.unamed.isDefined) {
      throw new CompilerException(unsupported_default_parameter)
    }

    true
  }

  def translate_operator(status : CompilerStatus):CompilerDefinedVariable = {

    val unified = super_variable_left.get.UNION(
      super_variable_right.get,
      input1.name,
      input2.get.name)

    CompilerDefinedVariable(output.name,output.pos,unified)
  }
}
