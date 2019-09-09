package it.polimi.genomics.compiler

import com.sun.org.apache.xalan.internal.xsltc.compiler.CompilerException
import it.polimi.genomics.core.DataStructures.CoverParameters.{ALL, CoverFlag, CoverParam, N}
import it.polimi.genomics.core.DataStructures.MetaJoinCondition.AttributeEvaluationStrategy
import it.polimi.genomics.core.DataStructures.RegionAggregate.RegionsToRegion

import scala.util.parsing.input.Position

/**
 * Created by pietro on 27/09/15.
 */
@SerialVersionUID(33L)
abstract class RegionIntersectionOperator2(op_pos : Position,
                          input1 : Variable,
                          input2 : Option[Variable] = None,
                          output : VariableIdentifier,
                          parameters : OperatorParameters)
  extends Operator(op_pos,input1, input2, parameters)
  with BuildingOperator2 with Serializable {

  override def check_input_number = one_input
  override val accepted_named_parameters = List("groupby", "aggregate")
  var minAcc : CoverParam = new N{override val n=0;}
  var maxAcc : CoverParam = new ALL{}

  var meta_group : Option[List[AttributeEvaluationStrategy]] = None
  var refined_agg_function_list : List[RegionsToRegion] = List.empty


  override def preprocess_operator(status: CompilerStatus) : Boolean = {
    super_variable_left = Some(get_variable_if_defined(input1.asInstanceOf[VariableIdentifier].name, status).get.payload)

    if (parameters.unamed.isDefined) {
      val cover_lims = parser_unnamed(cover_boundaries, None)
      minAcc = cover_lims.get._1
      maxAcc = cover_lims.get._2
    } else {
      val msg = operator_name + " operator at line " + op_pos.line + ": a value for minAcc and a value for maxAcc " +
        "are required."
      throw new CompilerException(msg)
    }
    for (p <- parameters.named) {
      p.param_name.trim.toLowerCase() match {
        case "groupby" => {
          meta_group = parser_named(rich_metadata_attribute_list,p.param_name.trim, p.param_value.trim)
        }
        case "aggregate" => {
          refined_agg_function_list =
            for (a <- parser_named(map_aggfun_list,p.param_name, p.param_value).get) yield {
            var fun : RegionsToRegion = null
            val new_field_name = if (a.output_field_name.isDefined) {
              Some(a.output_field_name.get.name)
            }else {
              None
            }
            val field_pos:Int = a.input_field_name match {
              case Some(FieldPosition(p)) => { left_var_check_num_field(p); p}
              case Some(FieldName(n)) => { left_var_get_field_name(n).get }
              case None => 0
            }

            if(new_field_name.isDefined) {
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
                val msg = operator_name + " operator at line " + op_pos.line + ": function name " + a.function_name +
                  " is not available"
                throw new CompilerException(msg)
            }

            fun
          }

        }
        case "at" => {
          parse_named_at(p.param_value)
        }
      }
    }

    true
  }
}
@SerialVersionUID(331L)
case class CoverOperator(op_pos : Position,
                                       input1 : Variable,
                                       input2 : Option[Variable] = None,
                                       output : VariableIdentifier,
                                       parameters : OperatorParameters)
  extends RegionIntersectionOperator2(op_pos,input1, input2, output, parameters)
  {
  override val operator_name = "COVER"

  def translate_operator(status : CompilerStatus):CompilerDefinedVariable = {

    val covered = super_variable_left.get.COVER(CoverFlag.COVER,minAcc,maxAcc,refined_agg_function_list,meta_group,
      operator_location)
    CompilerDefinedVariable(output.name,output.pos,covered)
  }
}
@SerialVersionUID(332L)
case class HistogramOperator(op_pos : Position,
                          input1 : Variable,
                          input2 : Option[Variable] = None,
                          output : VariableIdentifier,
                          parameters : OperatorParameters)
  extends RegionIntersectionOperator2(op_pos,input1, input2, output, parameters)
{
  override val operator_name = "HISTOGRAM"
  def translate_operator(status : CompilerStatus):CompilerDefinedVariable = {

    val covered = super_variable_left.get.COVER(CoverFlag.HISTOGRAM,
      minAcc,maxAcc,refined_agg_function_list,meta_group,
      operator_location)
    CompilerDefinedVariable(output.name,output.pos,covered)
  }

}
@SerialVersionUID(333L)
case class SummitOperator(op_pos : Position,
                          input1 : Variable,
                          input2 : Option[Variable] = None,
                          output : VariableIdentifier,
                          parameters : OperatorParameters)
  extends RegionIntersectionOperator2(op_pos,input1, input2, output, parameters)
{
  override val operator_name = "SUMMIT"
  def translate_operator(status : CompilerStatus):CompilerDefinedVariable = {

    val covered = super_variable_left.get.COVER(CoverFlag.SUMMIT,minAcc,maxAcc,refined_agg_function_list,meta_group,
      operator_location)
    CompilerDefinedVariable(output.name,output.pos,covered)
  }

}
@SerialVersionUID(334L)
case class FlatOperator(op_pos : Position,
                           input1 : Variable,
                           input2 : Option[Variable] = None,
                           output : VariableIdentifier,
                           parameters : OperatorParameters)
  extends RegionIntersectionOperator2(op_pos,input1, input2, output, parameters)
{
  override val operator_name = "FLAT"
  def translate_operator(status : CompilerStatus):CompilerDefinedVariable = {

    val covered = super_variable_left.get.COVER(
      CoverFlag.FLAT,
      minAcc,
      maxAcc,
      refined_agg_function_list,
      meta_group,
      operator_location)
    CompilerDefinedVariable(output.name,output.pos,covered)
  }

}
