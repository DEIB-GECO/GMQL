package it.polimi.genomics.compiler

import it.polimi.genomics.core.DataStructures.MetaJoinCondition.MetaJoinCondition
import it.polimi.genomics.core.DataStructures.{LOCAL_INSTANCE, MetaOperator}
import it.polimi.genomics.core.DataStructures.MetadataCondition.{META_OP, MetadataCondition, NOT, Predicate}
import it.polimi.genomics.core.DataStructures.RegionCondition.RegionCondition
import it.polimi.genomics.core.DataTypes.{FlinkMetaType, FlinkRegionType}
import it.polimi.genomics.core.{GMQLLoader, ParsingType}

import scala.util.parsing.input.Position

@SerialVersionUID(123125534332313121L)
case class SelectOperator(op_pos : Position,
                           input1 : Variable,
                           input2 : Option[Variable] = None,
                           output : VariableIdentifier,
                           parameters : OperatorParameters)
  extends Operator(op_pos,input1, input2, parameters)
  with BuildingOperator2 with Serializable
{

  override val operator_name = "SELECT"
  override val accepted_named_parameters = List("region","semijoin","parser")
  override def check_input_number = one_input

  var metadata_condition : Option[MetadataCondition] = None
  var semi_join_temp_condition : Option[MetaJoinConditionTemp] = None
  var region_temp_condition : Option[RegionCondition] = None
  var region_condition : Option[RegionCondition] = None
  var loader : Option[String] = None
  var sj_var : Option[MetaOperator] = None
  var sj_con : Option[MetaJoinCondition] = None

  override def check_input_variables(status : CompilerStatus) : Boolean = {

    true
  }

  override def preprocess_operator(status: CompilerStatus) : Boolean = {

    //Parser optional parameters
    for (n <- parameters.named) {
      n.param_name.trim.toLowerCase match {
        case "semijoin" => {
          semi_join_temp_condition = parser_named(select_sj_condition, n.param_name, n.param_value)
        }
        case "region" => {
          region_temp_condition = parser_named(region_select_expr, n.param_name,n.param_value)
        }
        case "parser" => {
          loader = parser_named(ident, n.param_name, n.param_value)
        }
        case "at" => {
          parse_named_at(n.param_value)
        }
      }
    }

    //parse the metadata condition or create a fake one
    metadata_condition = parser_unnamed(meta_select_expr,
      Some(it.polimi.genomics.core.DataStructures.MetadataCondition.NOT(
        Predicate("XvFC33", META_OP.EQ, "NjfdnD"))))


    //initialize super variable
    super_variable_left = input1 match {
      case p:VariablePath => Some({
        val sel_loader = status.get_server.implementation.getParser(loader.getOrElse("default"),p.path)
          .asInstanceOf[GMQLLoader[(Long,String), FlinkRegionType, (Long,String), FlinkMetaType]]
        status.get_server.READ(List(p.path), operator_location.getOrElse(LOCAL_INSTANCE)).USING(sel_loader)})
      case i:VariableIdentifier => Some({
        val var_in_scope = status.getVariable(i.name)
        if(var_in_scope.isDefined){
          var_in_scope.get.payload
        } else {
          logger.debug("i.name: " + i.name)
          val sel_loader = status.get_server.implementation.getParser(loader.getOrElse("default"),i.name)
            .asInstanceOf[GMQLLoader[(Long,String), FlinkRegionType, (Long,String), FlinkMetaType]]
          status.get_server.READ(List(i.name), operator_location.getOrElse(LOCAL_INSTANCE)).USING(sel_loader)
        }
      })
    }


    if(region_temp_condition.isDefined){
      region_condition = Some(refine_region_condition(region_temp_condition.get))
    }

    def refine_region_condition(rc: RegionCondition) : RegionCondition = {

      rc match {
        case x:it.polimi.genomics.core.DataStructures.RegionCondition.Predicate => {
          left_var_check_num_field(x.position)
          x
        }
        case RegionPredicateTemp(name,o,v) => {
          val fp = left_var_get_field_name(name)
          val ft = left_var_get_type_name(name)

          if (
            (v.isInstanceOf[java.lang.String] &&
              (ft.get == ParsingType.DOUBLE ||
                ft.get == ParsingType.INTEGER ||
                ft.get == ParsingType.LONG)) ||
              (!v.isInstanceOf[java.lang.String] &&
                (ft.get == ParsingType.STRING)))
          {
            val msg = "In " + operator_name + " at line " + op_pos.line + ", " +
              "incompatible types comparison.  Field '" + name + "'" +
              " is of type " + ft.get
            throw new CompilerException(msg)
          }

          it.polimi.genomics.core.DataStructures.RegionCondition.Predicate(fp.get,o,v)
        }
        case it.polimi.genomics.core.DataStructures.RegionCondition.AND(a,b) => {
          it.polimi.genomics.core.DataStructures.RegionCondition.AND(
            refine_region_condition(a),
            refine_region_condition(b))
        }
        case it.polimi.genomics.core.DataStructures.RegionCondition.OR(a,b) => {
          it.polimi.genomics.core.DataStructures.RegionCondition.OR(
            refine_region_condition(a),
            refine_region_condition(b))
        }
        case it.polimi.genomics.core.DataStructures.RegionCondition.NOT(a) => {
          it.polimi.genomics.core.DataStructures.RegionCondition.NOT(refine_region_condition(a))
        }
        case _ => rc
      }
    }


    sj_var = if(semi_join_temp_condition.isDefined){
      Some(get_variable_if_defined(semi_join_temp_condition.get.join_dataset_name.name, status).get.payload.metaDag)
    } else {
      None
    }
    sj_con = if(semi_join_temp_condition.isDefined){
      Some(
        MetaJoinCondition(
          semi_join_temp_condition.get.attributes,
          negation = semi_join_temp_condition.get.is_negated
        )
      )
    } else {
      None
    }

    true
  }

  override def translate_operator(status : CompilerStatus):CompilerDefinedVariable = {
    CompilerDefinedVariable(
      output.name,
      output.pos,
      super_variable_left
        .get
        .add_select_statement(
          sj_var,
          sj_con,
          metadata_condition,
          region_condition,
          operator_location))
  }

}