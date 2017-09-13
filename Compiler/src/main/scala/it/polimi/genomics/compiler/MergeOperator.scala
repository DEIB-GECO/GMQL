package it.polimi.genomics.compiler

import it.polimi.genomics.core.DataStructures.CoverParameters.{CoverFlag, CoverParam}
import it.polimi.genomics.core.DataStructures.MetaJoinCondition.AttributeEvaluationStrategy

import scala.util.parsing.input.Position

/**
 * Created by pietro on 27/09/15.
 */
@SerialVersionUID(18L)
case class MergeOperator(op_pos : Position,
                            input1 : Variable,
                            input2 : Option[Variable] = None,
                            output : VariableIdentifier,
                            parameters : OperatorParameters)
  extends Operator(op_pos,input1, input2, parameters)
  with BuildingOperator2 with Serializable {

  override val operator_name = "MERGE"
  override val accepted_named_parameters = List("groupby")

  var meta_group : Option[List[AttributeEvaluationStrategy]] = None

  override def check_input_number = one_input

  override def preprocess_operator(status: CompilerStatus) : Boolean = {
    super_variable_left = Some(get_variable_if_defined(input1.asInstanceOf[VariableIdentifier].name, status).get.payload)

    if (parameters.unamed.isDefined) {
      throw new CompilerException(unsupported_default_parameter)
    }
    for (p <- parameters.named) {
      p.param_name.trim.toLowerCase() match {
        case "groupby" => {
          meta_group = parser_named(rich_metadata_attribute_list,p.param_name.trim, p.param_value.trim)
        }

      }
    }

    true
  }

  override def translate_operator(status : CompilerStatus):CompilerDefinedVariable = {
    CompilerDefinedVariable(output.name,output.pos,super_variable_left.get.MERGE(meta_group))
  }
}