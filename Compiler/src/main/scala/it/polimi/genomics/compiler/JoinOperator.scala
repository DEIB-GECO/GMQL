package it.polimi.genomics.compiler

import it.polimi.genomics.core.DataStructures.GMQLOperator
import it.polimi.genomics.core.DataStructures.JoinParametersRD._
import it.polimi.genomics.core.DataStructures.JoinParametersRD.RegionBuilder.RegionBuilder
import it.polimi.genomics.core.DataStructures.MetaJoinCondition.MetaJoinCondition
import it.polimi.genomics.core.Debug.OperatorDescr

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
  override val accepted_named_parameters = List("joinby", "output", "on_attributes")

  var genometric_condition : List[JoinQuadruple] = List()
  var output_builder : RegionBuilder =  RegionBuilder.CONTIG
  var meta_join_param : Option[MetaJoinCondition] = None
  var on_attributes : Option[List[FieldPositionOrName]] = None
  var on_positions : Option[List[(Int, Int)]] = None

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

    }

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
        case "on_attributes" => {
          on_attributes = parser_named(join_on_attributes_condition ,n.param_name, n.param_value)

          val list_pairs:List[(Int, Int)] = for (a <- on_attributes.get) yield {
            a match {
              // in case it is a FieldPosition,
              // we assume it to be a coordinate attribute (i.e., chr)
              case FieldPosition(x) => (x,x)
              case FieldName(n) => {
                (left_var_get_field_name(n).get, right_var_get_field_name(n).get)
              }
            }
          }

          on_positions = Some(list_pairs)
        }

        case "output" => {
          val info = "Available options are: " +
            "CONTIG, INT, LEFT, RIGHT, RIGHT_DISTINCT and LEFT_DISTINCT. "
          output_builder = parser_named(region_builder, n.param_name, n.param_value, Some(info)).get
        }
        case "at" => {
          parse_named_at(n.param_value)
        }
      }
    }

    true
  }

  override def translate_operator(status : CompilerStatus):CompilerDefinedVariable = {

    //check the validity of parameters
    //
    //at least one of distal condition and attribute condition has to be defined
    if (genometric_condition.isEmpty && !on_positions.isDefined) {
      val msg = "JOIN operator at line " + op_pos.line +
        ": operators requires that at least one of distance condition or on_attributes condition " +
        "to be provided."
      throw new CompilerException(msg)
    }
    //
    //if the distal condition is present then there are no limitations on the builder
    //conversely, (when only the on_attributes is present) the accepted builder are LEFT and RIGHT
    if (genometric_condition.isEmpty &&
      on_positions.isDefined &&
      !(output_builder == RegionBuilder.LEFT ||
        output_builder == RegionBuilder.RIGHT ||
        output_builder == RegionBuilder.RIGHT_DISTINCT ||
        output_builder == RegionBuilder.LEFT_DISTINCT ||
        output_builder == RegionBuilder.BOTH)) {

      val msg = "JOIN operator at line " + op_pos.line +
        ": when a condition on distance is not provided, the only possible " +
        "region builders are LEFT, RIGHT, LEFT_DISTINCT, RIGHT_DISTINCT and BOTH."
      throw new CompilerException(msg)

    }


    var (max, min:String, stream, md:String) = (100000L, "NULL", "false", "NULL")

    genometric_condition.head.toList().foreach {
      case DistLess(v) => max = v
      case DistGreater(v) =>  min = v.toString
      case Upstream() => stream =  "true"
      case DownStream() =>  stream = "true"
      case MinDistance(v) => v.toString
    }


    val params: Option[Map[String,String]] = Some(Map(
      "output_type"->output_builder.toString,
      "DL"-> max.toString,
      "DG"-> min,
      "stream"->stream,
      "MD"->md
    ))
    val operatorDescr = OperatorDescr(GMQLOperator.Join, params)

    val joined = super_variable_left.get.JOIN(
      meta_join_param,
      genometric_condition,
      output_builder,
      super_variable_right.get,
      Some(input1.name),
      Some(input2.get.name),
      join_on_attributes = on_positions,
      operator_location,
      operatorDescription=operatorDescr
    )

    CompilerDefinedVariable(output.name,output.pos,joined)
  }

}
