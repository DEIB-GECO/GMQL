package it.polimi.genomics.compiler

import it.polimi.genomics.core.DataStructures.JoinParametersRD._
import it.polimi.genomics.core.DataStructures.JoinParametersRD.RegionBuilder.RegionBuilder
import it.polimi.genomics.core.DataStructures.MetaJoinCondition.MetaJoinCondition

import scala.util.parsing.input.Position

/**
 * Created by pietro on 27/09/15.
 */
@SerialVersionUID(12L)
case class JoinOperator(op_pos : Position,
                          input1 : Variable,
                          input2 : Option[Variable] = None,
                          output : VariableIdentifier,
                          parameters : OperatorParameters)
  extends Operator(op_pos,input1, input2, parameters)
  with BuildingOperator2 with Serializable {

  override val operator_name = "JOIN"
  override val accepted_named_parameters = List("joinby", "output")

  var genometric_condition : List[JoinQuadruple] = List(JoinQuadruple(Some(DistLess(0))))
  var output_builder : RegionBuilder =  RegionBuilder.CONTIG
  var meta_join_param : Option[MetaJoinCondition] = None

  override def check_input_number = two_inputs

  override def preprocess_operator(status: CompilerStatus) : Boolean = {
    super_variable_left = Some(get_variable_if_defined(input1.asInstanceOf[VariableIdentifier].name, status).get.payload)
    super_variable_right = Some(get_variable_if_defined(input2.get.asInstanceOf[VariableIdentifier].name, status).get.payload)
    val atomics = parser_unnamed(join_region_condition, None)

    if (atomics.isDefined) {
      if (atomics.size > 4) {
        val msg = "JOIN operator at line " + op_pos.line +
          ": region condition can be made of up to 4 atomic predicates. " +
          atomics.size + " provided."
        throw new CompilerException(msg)
      }

      val valid_condition = atomics.get
        .map({ x => if (x.isInstanceOf[DistLess] || x.isInstanceOf[MinDistance]) true else false })
        .reduce(_ || _)

      if (!valid_condition) {
        val msg = "JOIN operator at line " + op_pos.line +
          ": region condition must contain at least one \"min distance\" or \"distance less than\" atomic predicate. "
        throw new CompilerException(msg)
      }

      genometric_condition = List(atomics.get.size match {
        case 1 => JoinQuadruple(Some(atomics.get(0)))
        case 2 => JoinQuadruple(Some(atomics.get(0)), Some(atomics.get(1)))
        case 3 => JoinQuadruple(Some(atomics.get(0)), Some(atomics.get(1)), Some(atomics.get(2)))
        case 4 => JoinQuadruple(Some(atomics.get(0)), Some(atomics.get(1)), Some(atomics.get(2)), Some(atomics.get(3)))
      })

      for (n <- parameters.named) {
        n.param_name.trim.toLowerCase match {
          case "joinby" => {
            meta_join_param = Some(
              MetaJoinCondition(
                parser_named(
                  rich_metadata_attribute_list,
                  n.param_name,
                  n.param_value).get))
          }
          case "output" => {
            val info = "Available options are: CONTIG, INT, LEFT, RIGHT. "
            output_builder = parser_named(region_builder, n.param_name, n.param_value, Some(info)).get
          }
        }
      }

    }
    true
  }

  override def translate_operator(status : CompilerStatus):CompilerDefinedVariable = {

    val joined = super_variable_left.get.JOIN(
      meta_join_param,
      genometric_condition,
      output_builder,
      super_variable_right.get,
      Some(input1.name),
      Some(input2.get.name))

    CompilerDefinedVariable(output.name,output.pos,joined)
  }

}
