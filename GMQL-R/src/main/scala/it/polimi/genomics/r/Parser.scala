package it.polimi.genomics.r


import it.polimi.genomics.GMQLServer.{DefaultMetaExtensionFactory, GmqlServer}
import it.polimi.genomics.core.DataStructures.MetadataCondition.{META_OP, MetadataCondition}
import it.polimi.genomics.compiler._
import it.polimi.genomics.core.DataStructures.CoverParameters.CoverParam
import it.polimi.genomics.core.DataStructures.IRVariable
import it.polimi.genomics.core.DataStructures.RegionAggregate._
import it.polimi.genomics.core.DataStructures.RegionCondition.RegionCondition
import it.polimi.genomics.core.ParsingType
import it.polimi.genomics.core.ParsingType.PARSING_TYPE
import it.polimi.genomics.core.DataStructures.MetaAggregate.MetaExtension
import it.polimi.genomics.core.DataStructures.MetadataCondition.META_OP.META_OP





class Parser(input_var: IRVariable, server: GmqlServer) extends GmqlParsers {

  val super_variable_left = input_var
  val gmql_server = server


  def this()
  {
    this(null,null)
  }
  val value:Parser[String] = floatingPointNumber |
    (stringLiteral ^^ {x=> x} | """[a-zA-Z0-9_\\*\\+\\-]+""".r ^^{x => x} )

  //val meta_attribute:Parser[String] = rep1sep(ident, ".") ^^ {_.mkString(".")}
  val operator:Parser[String] = "==" | "!="  | ">=" | "<=" | ">" | "<"
  val attribute:Parser[String] = rep1sep(ident, ".") ^^ {_.mkString(".")}
  val cond:Parser[String] = (attribute ~ operator ~ value ) ^^ {x => x._1._1 + x._1._2 + x._2}
  val factor: Parser[String] = "(" ~> expr <~ ")" ^^ {x=> "(" + x + ")"} |
    (("!" ~ "(" )~> expr <~ ")") ^^ { x => "NOT(" +x+ ")"  }  |
    ("!" ~> cond ) ^^ { x => "NOT(" +x+ ")"  } | cond

  val term:Parser[String] = factor ~ (("AND" ~> factor)*) ^^ { x =>
    if (x._2.isEmpty)
      x._1
    else {
      (List(x._1) ++ x._2).reduce((x, y) => x + " AND " + y)
    }
  }
  val expr:Parser[String] = term ~ (("OR" ~> term)*) ^^ { x =>
    if (x._2.isEmpty)
      x._1
    else {
      (List(x._1) ++ x._2).reduce((x, y) => x + " OR " + y)
    }
  }

  val chr_cond:Parser[String] = CHR ~> "==" ~> """[a-zA-Z0-9_\\*\\+\\-]+""".r ^^ {x =>"chr == "+ x}
  val left_cond:Parser[String] = LEFT ~> (operator ~ decimalNumber) ^^ {x=> "left" + x._1 + x._2.toLong}
  val right_cond:Parser[String] = RIGHT ~> (operator ~ decimalNumber) ^^ {x=> "right" + x._1 + x._2.toLong}
  val stop_cond:Parser[String] = STOP ~> (operator ~ decimalNumber) ^^ {x=> "stop" + x._1 + x._2.toLong}
  val start_cond:Parser[String] = START ~> (operator ~ decimalNumber) ^^ {x=> "start" + x._1 + x._2.toLong}
  val strand_cond:Parser[String] = STRAND ~> "==" ~> stringLiteral ^^ {x=> "strand" + "==" + x}
  val schema_cond:Parser[String] = cond


  val meta_cond:Parser[String] = (attribute ~ operator ~ ("META" ~> "(" ~>  value_reg <~ ")") )^^
    { x => x._1._1 +x._1._2 + "META(" + x._2.drop(1).dropRight(1)+ ")" }
  val value_reg:Parser[String] = """[a-zA-Z0-9_\\*\\+\\-]+""".r ^^{x => x} | floatingPointNumber|
   stringLiteral ^^ {x=> x}
  val cond_reg:Parser[String] = chr_cond | strand_cond | start_cond | stop_cond | left_cond | right_cond |
    meta_cond | cond
  val factor_reg: Parser[String] = "(" ~> expr_reg <~ ")" ^^ {x=> "(" + x + ")"} |
    (("!" ~ "(" )~> expr_reg <~ ")") ^^ { x => "NOT(" +x+ ")"  }  |
    ("!" ~> cond_reg ) ^^ { x => "NOT(" +x+ ")"  }  | cond_reg

  val term_reg:Parser[String] = factor_reg ~ (("AND" ~> factor_reg)*) ^^ { x =>
    if (x._2.isEmpty)
      x._1
    else {
      (List(x._1) ++ x._2).reduce((x, y) => x + " AND " + y)
    }
  }
  val expr_reg:Parser[String] = term_reg ~ (("OR" ~> term_reg)*) ^^ { x =>
    if (x._2.isEmpty)
      x._1
    else
      (List(x._1) ++ x._2).reduce((x, y) => x + " OR " + y)
  }


  val cover_exp:Parser[String] = "ALL" ~> "+" ~> wholeNumber ~ ( "/" ~> wholeNumber) ^^ {
    x => "(ALL + " + x._1.toInt + ")" +"/" + x._2.toInt } |
    "(" ~ "ALL" ~> "+" ~> wholeNumber ~ ")" ~ ( "/" ~> wholeNumber) ^^ {
      x => "(ALL + " + x._1._1.toInt + ")" +"/" + x._2.toInt } |
    wholeNumber ^^ { x => x} |
    "ALL" ~ "/" ~> wholeNumber ^^ {x => "ALL / "+x} |
    "ALL" ^^ {x => x} | "ANY" ^^ {x=> x}



  def findAndChangeMeta(input:String): String =
  {
    val metadata = parse(expr, input)

    metadata match {
      case Success(result, next) => result
      case NoSuccess(result, next) => "Invalid Syntax"
      case Error(result, next) => "Invalid Syntax"
      case Failure(result, next) => "Failure"
    }
  }

  def findAndChangeReg(input:String): String =
  {
    val region = parse(expr_reg, input)
    //println(region)
    region match {
      case Success(result, next) => result
      case NoSuccess(result, next) => "Invalid Syntax"
      case Error(result, next) => "Invalid Syntax"
      case Failure(result, next) => "Failure"
    }
  }

  def findAndChangeCover(input:String): String =
  {
    val param = parse(cover_exp, input)

    param match {
      case Success(result, next) => result
      case NoSuccess(result, next) => "Invalid Syntax"
      case Error(result, next) => "Invalid Syntax"
      case Failure(result, next) => "Failure"
    }
  }

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
  def parseCoverParam(input: String): (String,CoverParam) = {

    val coverParam = parse(cover_param, input)
    coverParam match {
      case Success(result, next) => ("OK", result)
      case NoSuccess(result, next) => (result, null)
      case Error(result, next) => (result, null)
      case Failure(result, next) => (result, null)
    }
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

  def parseProjectMetdata(input: String):  (String,Option[List[MetaExtension]]) = {

    val extend_meta = parse(metadata_modifier_list, input)
    var meta_modifier: Option[List[MetaExtension]] = None
    meta_modifier= Some(
      extend_meta.get.map(
        {parsed_modifier =>
          DefaultMetaExtensionFactory.get(
            parsed_modifier.dag,
            parsed_modifier.output)}
      )
    )
    ("OK",meta_modifier)
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
      case READD(a,b) => READD(refineREDag(a), refineREDag(b))
      case RESUB(a,b) => RESUB(refineREDag(a), refineREDag(b))
      case REMUL(a,b) => REMUL(refineREDag(a), refineREDag(b))
      case REDIV(a,b) => REDIV(refineREDag(a), refineREDag(b))
      case RESQRT(a) => RESQRT(refineREDag(a))
      case _ => dag
    }
  }

}