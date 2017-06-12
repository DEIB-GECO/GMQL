package it.polimi.genomics.r


import it.polimi.genomics.GMQLServer.GmqlServer
import it.polimi.genomics.core.DataStructures.MetadataCondition.MetadataCondition
import it.polimi.genomics.compiler._
import it.polimi.genomics.core.DataStructures.CoverParameters.CoverParam
import it.polimi.genomics.core.DataStructures.IRVariable
import it.polimi.genomics.core.DataStructures.RegionAggregate._
import it.polimi.genomics.core.DataStructures.RegionCondition.RegionCondition
import it.polimi.genomics.core.ParsingType
import it.polimi.genomics.core.ParsingType.PARSING_TYPE

import scala.util.parsing.input.CharSequenceReader


class Parser(input_var: IRVariable, server: GmqlServer) extends GmqlParsers {

  val super_variable_left = input_var
  val gmql_server = server

  def parseSelectMetadata(input: String): (String, Option[MetadataCondition]) = {

    //println("input : " + input)
    val metadata = parse(meta_select_expr, input)

    metadata match {
      case Success(result, next) => ("OK", Some(result))
      case NoSuccess(result, next) => (result, None)
      case Error(result, next) => (result, None)
      case Failure(result, next) => (result, None)
    }
  }

  def parseSelectRegions(input: String): (String, Option[RegionCondition]) = {

    //println("input : " + input)
    val regions = parse(region_select_expr, input)

    regions match {
      case Success(result, next) => {
        var condition = result
        try {
          condition = refine_region_condition(result)
        } catch {
          case ce: CompilerException => return (ce.getMessage, None)
        }
        ("OK", Some(condition))
      }
      case NoSuccess(result, next) => (result, None)
      case Error(result, next) => (result, None)
      case Failure(result, next) => (result, None)
    }

  }

  def parseCoverParam(input: String): Unit = {

  }

  def refine_region_condition(rc: RegionCondition): RegionCondition = {

    rc match {
      case x: it.polimi.genomics.core.DataStructures.RegionCondition.Predicate => {
        left_var_check_num_field(x.position)
        x
      }
      case RegionPredicateTemp(name, o, v) => {
        val fp = left_var_get_field_name(name)
        val ft = left_var_get_type_name(name)

        if (
          (v.isInstanceOf[java.lang.String] &&
            (ft.get == ParsingType.DOUBLE ||
              ft.get == ParsingType.INTEGER ||
              ft.get == ParsingType.LONG)) ||
            (!v.isInstanceOf[java.lang.String] &&
              (ft.get == ParsingType.STRING))) {
          val msg = "No field '" + name + "'" + " is of type " + ft.get
          throw new CompilerException(msg)
        }

        it.polimi.genomics.core.DataStructures.RegionCondition.Predicate(fp.get, o, v)
      }
      case it.polimi.genomics.core.DataStructures.RegionCondition.AND(a, b) => {
        it.polimi.genomics.core.DataStructures.RegionCondition.AND(
          refine_region_condition(a),
          refine_region_condition(b))
      }
      case it.polimi.genomics.core.DataStructures.RegionCondition.OR(a, b) => {
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

  def left_var_check_num_field(pos: Int): Boolean = {
    if (pos < super_variable_left.get_number_of_fields) {
      true
    } else {
      val msg = "No field at position " + pos
      throw new CompilerException(msg)
      false
    }
  }

  def left_var_get_field_name(name: String): Option[Int] = {
    val fp = super_variable_left.get_field_by_name(name)
    if (fp.isDefined) {
      fp
    }
    else {
      val schema_list = super_variable_left.schema.map(_._1).mkString(", ")
      val msg = "No field " + name + " defined. Avalilable fields are { " + schema_list + " } "
      throw new CompilerException(msg)
      None
    }
  }

  def left_var_get_type_name(name: String): Option[PARSING_TYPE] = {
    val fp = super_variable_left.get_type_by_name(name)
    if (fp.isDefined) {
      fp
    }
    else {
      val schema_list = super_variable_left.schema.map(_._1).mkString(", ")
      val msg = "No field " + name + " defined. Avalilable fields are { " + schema_list + " } "
      throw new CompilerException(msg)
      None
    }
  }

  def getListRegionsFunction(input:String): Unit =
  {

  }


  def parseProjectRegion(input: String): (String, Option[List[RegionFunction]]) = {

    //println("input : " + input)
    val extend_regions = parse(region_modifier_list, input)
    var region_modifier: Option[List[RegionFunction]] = None

    extend_regions match {
      case Success(result, next) => {
        try {
          region_modifier = {
            val allNames = result
              .filter(_.isInstanceOf[RegionModifier])
              .map(_.asInstanceOf[RegionModifier].field)
              .filter(_.isInstanceOf[FieldName])
              .map(_.asInstanceOf[FieldName].name)
            if (allNames.toSet.size != allNames.size) {
              val msg = "Some field name has been repeated multiple times"
              throw new CompilerException(msg)
            }
            val l = result
              .filter(_.isInstanceOf[RegionModifier])
              .map(x =>
                gmql_server.implementation.regionExtensionFactory.get(
                  refineREDag(x.asInstanceOf[RegionModifier].dag),
                  x.asInstanceOf[RegionModifier].field match {
                    case FieldPosition(p) => {
                      left_var_check_num_field(p)
                      Right(p)
                    }
                    case FieldName(n) => {
                      try {
                        val p = left_var_get_field_name(n).get
                        Right(p)
                      } catch {
                        case e: Exception => Left(n)
                      }
                    }
                  }
                ))
            if (l.isEmpty)
              None
            else
              Some(l)
          }

        }catch{
          case ce: CompilerException => return (ce.getMessage, None)
        }
        ("OK",region_modifier)
      }
      case NoSuccess(result, next) => (result, None)
      case Error(result, next) => (result, None)
      case Failure(result, next) => (result, None)
    }
  }

  def refineREDag(dag: RENode): RENode = {
    dag match {
      case REFieldNameOrPosition(f) => {
        if (f.isInstanceOf[FieldPosition]) {
          val p = f.asInstanceOf[FieldPosition].pos
          left_var_check_num_field(p)
          REPos(p)
        }
        else {
          val p = left_var_get_field_name(f.asInstanceOf[FieldName].name).get
          REPos(p)
        }
      }
      case READD(a, b) => READD(refineREDag(a), refineREDag(b))
      case RESUB(a, b) => RESUB(refineREDag(a), refineREDag(b))
      case REMUL(a, b) => REMUL(refineREDag(a), refineREDag(b))
      case REDIV(a, b) => REDIV(refineREDag(a), refineREDag(b))
      case _ => dag
    }
  }

}