package it.polimi.genomics.compiler

import it.polimi.genomics.core.DataStructures.GroupRDParameters.{FIELD, GroupingParameter}
import it.polimi.genomics.core.DataStructures.MetaGroupByCondition.MetaGroupByCondition
import it.polimi.genomics.core.DataStructures.RegionAggregate.{RegionsToRegion, RegionsToMeta}

import scala.util.parsing.input.Position

/**
 * Created by pietro on 28/09/15.
 */
/*
@SerialVersionUID(28L)
case class GroupOperator(op_pos : Position,
                          input1 : Variable,
                          input2 : Option[Variable] = None,
                          output : VariableIdentifier,
                          meta_grouping : Option[GroupMetaParameters],
                          region_grouping : Option[GroupRegionParameters]
                           ) extends Operator with Serializable with BuildingOperator {

  var refined_meta_aggregate_function_list: List[RegionsToMeta] = List.empty
  var refined_region_aggregate_function_list : List[RegionsToRegion] = List.empty
  var list_region_grouping_parameters : Option[List[GroupingParameter]] = None
  var list_meta_grouping_parameters : Option[MetaGroupByCondition] = None

  @throws[CompilerException]
  override def check_input_number(): Boolean = {

    if (input2.isDefined) {
      val msg = "GROUP at line " + op_pos.line + ": wrong number of input variables. Required 1, provided 2."
      throw new CompilerException(msg)
    }
    true
  }

  @throws[CompilerException]
  def preprocess_operator(status: CompilerStatus) : Boolean = {
    val reference = status.getVariable(input1.asInstanceOf[VariableIdentifier].name).get

    val meta_aggregates = if(meta_grouping.isDefined) meta_grouping.get.aggregates.getOrElse(List.empty) else List.empty
    refined_meta_aggregate_function_list = for (a <- meta_aggregates) yield {

      val position:Int = a.input_field_name match {
        case FieldPosition(p) => {if(p>=reference.payload.get_number_of_fields){
          val msg = "GROUP at line " + op_pos.line + ": field position " + p + " is not available. "
          throw new CompilerException(msg)
        } else {p}
        }
        case FieldName(n) => {
          val p = reference.payload.get_field_by_name(n)
          if (!p.isDefined) {
            val msg = "GROUP operator at line " + op_pos.line + ": field name " + n +
              " is not available in the schema of variable " + reference.name
            throw new CompilerException(msg)
          }else{
            p.get
          }
        }
      }
      var fun : RegionsToMeta = null
      try {
        fun = status.get_server.implementation.extendFunctionFactory.get(a.function_name,position,a.output_attribute_name)
        fun.function_identifier = a.function_name
        fun.input_index = position
        fun.output_attribute_name = a.output_attribute_name.getOrElse("default_aggregate")
      } catch {
        case e:Exception => throw new CompilerException(e.getMessage)
      }
      fun
    }

    val region_aggregates = if (region_grouping.isDefined) region_grouping.get.aggregates.getOrElse(List.empty) else List.empty
    refined_region_aggregate_function_list =
      for (a <- region_aggregates) yield{
        var fun : RegionsToRegion = null
        val new_field_name = if (a.output_field_name.isDefined) {
          Some(a.output_field_name.get.name)
        }else {
          None
        }
        val field_pos:Int = a.input_field_name match {
          case FieldPosition(p) => {
            if (p>=reference.payload.get_number_of_fields ) {
              val msg = "GROUP operator at line " + op_pos.line + ": field position " + p +
                " is not available in the schema of variable " + reference.name
              throw new CompilerException(msg)
            }
            else{
              p
            }
          }
          case FieldName(n) => {
            val p = reference.payload.get_field_by_name(n)
            if (!p.isDefined) {
              val msg = "GROUP operator at line " + op_pos.line + ": field name " + n +
                " is not available in the schema of variable " + reference.name
              throw new CompilerException(msg)
            }else{
              p.get
            }
          }
        }

        if(new_field_name.isDefined){
          if (status.getVariable(reference.name).get.payload.get_field_by_name(new_field_name.get).isDefined){
            val msg = "GROUP operator at line " + op_pos.line + ": field name " + new_field_name.get + " " +
              "already present. Cannot be redefined."
            throw new CompilerException(msg)
          }
        }

        try {
          fun = status.get_server.implementation
            .mapFunctionFactory.get(a.function_name,field_pos,new_field_name)
          fun.function_identifier = a.function_name
          fun.input_index = field_pos
          fun.output_name = new_field_name
        } catch {
          case e:Exception =>
            val msg = "GROUP operator at line " + op_pos.line + ": function name " + a.function_name +
              " is not available"
            throw new CompilerException(msg)
        }

        fun
      }

    val dup_fields_name = refined_region_aggregate_function_list
      .map(x=>x.fieldName.get)
      .groupBy(identity)
      .collect { case (x,ys) if ys.size > 1 => x }

    for (d <- dup_fields_name) {
      val msg = "GROUP operator at line " + op_pos.line + ": field name " + d + " " +
        "has multiple definitions."
      throw new CompilerException(msg)
    }

    list_region_grouping_parameters = if (!(region_grouping.isDefined && region_grouping.get.grouping.isDefined)) {
      None
    } else {
      Some(for (f <- region_grouping.get.grouping.get) yield {
        f match {
          case FieldPosition(p) => {if(p>=reference.payload.get_number_of_fields){
            val msg = "GROUP at line " + op_pos.line + ": field position " + p + " is not available. "
            throw new CompilerException(msg)
          } else {
            FIELD(p)
          }
          }
          case FieldName(n) => {
            val p = reference.payload.get_field_by_name(n)
            if (!p.isDefined) {
              val msg = "GROUP operator at line " + op_pos.line + ": field name " + n +
                " is not available in the schema of variable " + reference.name
              throw new CompilerException(msg)
            }else{
              FIELD(p.get)
            }
          }
        }
      })
    }
    if (meta_grouping.isDefined && meta_grouping.get.grouping.isDefined) {
      list_meta_grouping_parameters = Some(meta_grouping.get.grouping.get)
    }
    true
  }

  def translate_operator(status : CompilerStatus):CompilerDefinedVariable = {

    val reference = status.getVariable(input1.asInstanceOf[VariableIdentifier].name).get

    val grouped = reference.payload.GROUP(
      list_meta_grouping_parameters,
      if(refined_meta_aggregate_function_list.isEmpty) None else Some(refined_meta_aggregate_function_list),
      "_group",
      list_region_grouping_parameters,
      if(refined_region_aggregate_function_list.isEmpty) None else Some(refined_region_aggregate_function_list))


    CompilerDefinedVariable(output.name,output.pos,grouped)
  }

}
*/