package it.polimi.genomics.compiler


import it.polimi.genomics.GMQLServer.GmqlServer
import it.polimi.genomics.core.DataStructures.MetaGroupByCondition.MetaGroupByCondition
import it.polimi.genomics.core.DataStructures.MetadataCondition.{MetadataCondition, META_OP}
import it.polimi.genomics.core.DataStructures.RegionCondition._
import scala.util.parsing.input.{CharSequenceReader, Positional}


class Translator(server: GmqlServer, output_path : String) extends GmqlParsers {


  val statement_separator: Parser[String] = ";"
  val assignment_symbol: Parser[String] = "="

  val operatorInput: Parser[(Variable, Option[Variable])] =
    variableId ~ variableId ^^ { x => (x._1, Some(x._2)) } |
      variableId ^^ { x => (x, None) }


  val function_name: Parser[String] = ident


  val GROUPBY: Parser[String] = """[g|G][r|R][o|O][u|U][p|P][b|B][y|Y]""".r
  val JOINBY: Parser[String] = """[j|J][o|O][i|I][n|N][b|B][y|Y]""".r
  val MATERIALIZE_token: Parser[String] =
    """[m|M][a|A][t|T][e|E][r|R][i|I][a|A][l|L][i|I][z|Z][e|E]""".r ^^ {
      _.toUpperCase
    }
  val SELECT_token: Parser[String] =
    """[s|S][e|E][l|L][e|E][c|C][t|T]""".r ^^ {
      _.toUpperCase
    }
  val PROJECT_token: Parser[String] =
    """[p|P][r|R][o|O][j|J][e|E][c|C][t|T]""".r ^^ {
      _.toUpperCase
    }
  val MAP_token: Parser[String] =
    """[m|M][a|A][p|P]""".r ^^ {
      _.toUpperCase
    }
  val JOIN_token: Parser[String] =
    """[j|J][o|O][i|I][n|N]""".r ^^ {
      _.toUpperCase
    }
  val HISTOGRAM_token: Parser[String] =
    ("""[c|C][o|O][v|V][e|E][r|R]_[h|H][i|I][s|S][t|T][o|O][g|G][r|R][a|A][m|M]""".r |
      """[h|H][i|I][s|S][t|T][o|O][g|G][r|R][a|A][m|M]""".r) ^^ {
      _.toUpperCase
    }
  val SUMMIT_token: Parser[String] =
    ("""[c|C][o|O][v|V][e|E][r|R]_[s|S][u|U][m|M][m|M][i|I][t|T]""".r |
      """[s|S][u|U][m|M][m|M][i|I][t|T]""".r) ^^ {
      _.toUpperCase
    }
  val FLAT_token: Parser[String] =
    ("""[c|C][o|O][v|V][e|E][r|R]_[f|F][l|L][a|A][t|T]""".r |
      """[f|F][l|L][a|A][t|T]""".r) ^^ {
      _.toUpperCase
    }
  val COVER_token: Parser[String] =
    ("""[c|C][o|O][v|V][e|E][r|R]""".r) ^^ {
      _.toUpperCase
    }
  val DIFFERENCE_token: Parser[String] =
    """[d|D][i|I][f|F][f|F][e|E][r|R][e|E][n|N][c|C][e|E]""".r ^^ {
      _.toUpperCase
    }
  val MERGE_token: Parser[String] =
    """[m|M][e|E][r|R][g|G][e|E]""".r ^^ {
      _.toUpperCase
    }
  val EXTEND_token: Parser[String] =
    """[e|E][x|X][t|T][e|E][n|N][d|D]""".r ^^ {
      _.toUpperCase
    }
  val UNION_token: Parser[String] =
    """[u|U][n|N][i|I][o|O][n|N]""".r ^^ {
      _.toUpperCase
    }
  val ORDER_token: Parser[String] =
    """[o|O][r|R][d|D][e|E][r|R]""".r ^^ {
      _.toUpperCase
    }
  val GROUP_token: Parser[String] =
    """[g|G][r|R][o|O][u|U][p|P]""".r ^^ {
      _.toUpperCase
    }

  val MATERIALIZE: Parser[ParsedOperatorToken] = positioned(MATERIALIZE_token ^^ ParsedOperatorToken)
  val SELECT: Parser[ParsedOperatorToken] = positioned(SELECT_token ^^ ParsedOperatorToken)
  val PROJECT: Parser[ParsedOperatorToken] = positioned(PROJECT_token ^^ ParsedOperatorToken)
  val MAP: Parser[ParsedOperatorToken] = positioned(MAP_token ^^ ParsedOperatorToken)
  val JOIN: Parser[ParsedOperatorToken] = positioned(JOIN_token ^^ ParsedOperatorToken)
  val HISTOGRAM: Parser[ParsedOperatorToken] = positioned(HISTOGRAM_token ^^ ParsedOperatorToken)
  val SUMMIT: Parser[ParsedOperatorToken] = positioned(SUMMIT_token ^^ ParsedOperatorToken)
  val FLAT: Parser[ParsedOperatorToken] = positioned(FLAT_token ^^ ParsedOperatorToken)
  val COVER: Parser[ParsedOperatorToken] = positioned(COVER_token ^^ ParsedOperatorToken)
  val DIFFERENCE: Parser[ParsedOperatorToken] = positioned(DIFFERENCE_token ^^ ParsedOperatorToken)
  val MERGE: Parser[ParsedOperatorToken] = positioned(MERGE_token ^^ ParsedOperatorToken)
  val EXTEND: Parser[ParsedOperatorToken] = positioned(EXTEND_token ^^ ParsedOperatorToken)
  val UNION: Parser[ParsedOperatorToken] = positioned(UNION_token ^^ ParsedOperatorToken)
  val ORDER: Parser[ParsedOperatorToken] = positioned(ORDER_token ^^ ParsedOperatorToken)
  val GROUP: Parser[ParsedOperatorToken] = positioned(GROUP_token ^^ ParsedOperatorToken)
  val WRONG: Parser[ParsedOperatorToken] = positioned(ident ^^ ParsedOperatorToken)
  val OPERATOR: Parser[ParsedOperatorToken] =
    SELECT | PROJECT | MAP | JOIN | HISTOGRAM | SUMMIT | FLAT | COVER | MERGE | EXTEND | ORDER | GROUP

  val comparison: Parser[String] = ("==" | "!=" | ">=" | "<=" | ">" | "<")
  val arithmetic: Parser[String] =
      STAR |
      MULT |
      DIV |
      SUB |
      SUM

  val keyword: Parser[String] = INTERSECT | ASC | OR | AND | NOT | LEFT | RIGHT | STRAND | CHR |
    START | STOP | META | UPSTREAM | DOWNSTREAM | MINDIST | DISTANCE | DISTLESS |
    DISTGREATER | IN | CONTIG | ANY | ALL | TOPG | TOP | AS | DESC
  val gmql_identifier: Parser[String] = rep1sep(ident | "?", ".") ^^ {
    _.mkString(".")
  }

  val param_value: Parser[String] =
    rep1(
      comparison |
        arithmetic |
        //metadata_attribute |
        gmql_identifier |
        decimalNumber |
        floatingPointNumber |
        "," |
        stringLiteral |
        "(" ~ param_value ~ ")" ^^ (x => x._1._1 + x._1._2 + x._2) |
        "(" ~ ")" ^^ (x => x._1 + x._2)
    ) ^^ {
    _.mkString(" ")
  }

  val named_parameter: Parser[NamedParameter] = (ident <~ ":") ~ param_value ^^ {
    x => NamedParameter(x._1, x._2)
  }
  val unnamed_parameter: Parser[String] = param_value
  val named_parameters: Parser[List[NamedParameter]] = rep1sep(named_parameter, ";")
  val operator_parameters: Parser[OperatorParameters] =
    ((unnamed_parameter <~ ";") ~ named_parameters) ^^ { x => OperatorParameters(Some(x._1), x._2) } |
      named_parameters ^^ {
        OperatorParameters(None, _)
      } |
      unnamed_parameter ^^ { x => OperatorParameters(Some(x), List.empty) } |
      "" ^^ { x => OperatorParameters(None, List.empty) }


  val metadata_groupby_list: Parser[List[String]] = GROUPBY ~> metadata_attribute_list
  val metadata_joinby_list: Parser[List[String]] = JOINBY ~> metadata_attribute_list

  val select_metadata_condition: Parser[MetadataCondition] = meta_select_expr |
    STAR ^^ { x =>
      it.polimi.genomics.core.DataStructures.MetadataCondition.NOT(
        it.polimi.genomics.core.DataStructures.MetadataCondition.Predicate("asdfg", META_OP.EQ, "qwerty123"))
    }

  val select_parameters: Parser[(Option[MetaJoinConditionTemp], Option[MetadataCondition], Option[RegionCondition])] =
    ((select_sj_condition <~ ";").? ~ (select_metadata_condition).?) ~ ((";" ~> region_select_expr).?) ^^ { x => (x._1._1, x._1._2, x._2) }


  val selectStatement =
    ((variableId <~ assignment_symbol) ~ ((SELECT <~ "(") ~ operator_parameters <~ ")")) ~
      ( (onlyPath ^^ {
        (_, None)
      })| operatorInput) <~ ";" ^^ {
      x => SelectOperator(x._1._2._1.pos, x._2._1, x._2._2, x._1._1, x._1._2._2)
    }

  val projectStatement =
    ((variableId <~ assignment_symbol) ~ ((PROJECT <~ "(") ~ operator_parameters <~ ")")) ~
      (operatorInput | (onlyPath ^^ {
        (_, None)
      })) <~ ";" ^^ {
      x => ProjectOperator(x._1._2._1.pos, x._2._1, x._2._2, x._1._1, x._1._2._2)
    }

  val mergeStatement: Parser[Operator] =
    ((variableId <~ assignment_symbol) ~ ((MERGE <~ "(") ~ operator_parameters <~ ")")) ~
      (operatorInput | (onlyPath ^^ {
        (_, None)
      })) <~ ";" ^^ {
      x => MergeOperator(x._1._2._1.pos, x._2._1, x._2._2, x._1._1, x._1._2._2)
    }

  val joinStatement: Parser[Operator] =
    ((variableId <~ assignment_symbol) ~ ((JOIN <~ "(") ~ operator_parameters <~ ")")) ~
      (operatorInput | (onlyPath ^^ {
        (_, None)
      })) <~ ";" ^^ {
      x => JoinOperator(x._1._2._1.pos, x._2._1, x._2._2, x._1._1, x._1._2._2)
    }

  val extendStatement: Parser[Operator] =
    ((variableId <~ assignment_symbol) ~ ((EXTEND <~ "(") ~ operator_parameters <~ ")")) ~
      (operatorInput | (onlyPath ^^ {
        (_, None)
      })) <~ ";" ^^ {
      x => ExtendOperator(x._1._2._1.pos, x._2._1, x._2._2, x._1._1, x._1._2._2)
    }

  val orderStatement: Parser[Operator] =
    ((variableId <~ assignment_symbol) ~ ((ORDER <~ "(") ~ operator_parameters <~ ")")) ~
      (operatorInput | (onlyPath ^^ {
        (_, None)
      })) <~ ";" ^^ {
      x => OrderOperator(x._1._2._1.pos, x._2._1, x._2._2, x._1._1, x._1._2._2)
    }

  val coverStatement: Parser[Operator] =
    ((variableId <~ assignment_symbol) ~ ((COVER <~ "(") ~ operator_parameters <~ ")")) ~
      (operatorInput | (onlyPath ^^ {
        (_, None)
      })) <~ ";" ^^ {
      x => CoverOperator(x._1._2._1.pos, x._2._1, x._2._2, x._1._1, x._1._2._2)
    }

  val summitStatement: Parser[Operator] =
    ((variableId <~ assignment_symbol) ~ ((SUMMIT <~ "(") ~ operator_parameters <~ ")")) ~
      (operatorInput | (onlyPath ^^ {
        (_, None)
      })) <~ ";" ^^ {
      x => SummitOperator(x._1._2._1.pos, x._2._1, x._2._2, x._1._1, x._1._2._2)
    }

  val flatStatement: Parser[Operator] =
    ((variableId <~ assignment_symbol) ~ ((FLAT <~ "(") ~ operator_parameters <~ ")")) ~
      (operatorInput | (onlyPath ^^ {
        (_, None)
      })) <~ ";" ^^ {
      x => FlatOperator(x._1._2._1.pos, x._2._1, x._2._2, x._1._1, x._1._2._2)
    }

  val histogramStatement: Parser[Operator] =
    ((variableId <~ assignment_symbol) ~ ((HISTOGRAM <~ "(") ~ operator_parameters <~ ")")) ~
      (operatorInput | (onlyPath ^^ {
        (_, None)
      })) <~ ";" ^^ {
      x => HistogramOperator(x._1._2._1.pos, x._2._1, x._2._2, x._1._1, x._1._2._2)
    }

  val mapStatement: Parser[Operator] =
    ((variableId <~ assignment_symbol) ~ ((MAP <~ "(") ~ operator_parameters <~ ")")) ~
      (operatorInput | (onlyPath ^^ {
        (_, None)
      })) <~ ";" ^^ {
      x => MapOperator(x._1._2._1.pos, x._2._1, x._2._2, x._1._1, x._1._2._2)
    }

  val differenceStatement: Parser[Operator] =
    ((variableId <~ assignment_symbol) ~ ((DIFFERENCE <~ "(") ~ operator_parameters <~ ")")) ~
      (operatorInput | (onlyPath ^^ {
        (_, None)
      })) <~ ";" ^^ {
      x => DifferenceOperator(x._1._2._1.pos, x._2._1, x._2._2, x._1._1, x._1._2._2)
    }

  val unionStatement: Parser[Operator] =
    ((variableId <~ assignment_symbol) ~ ((UNION <~ "(") ~ operator_parameters <~ ")")) ~
      (operatorInput | (onlyPath ^^ {
        (_, None)
      })) <~ ";" ^^ {
      x => UnionOperator(x._1._2._1.pos, x._2._1, x._2._2, x._1._1, x._1._2._2)
    }

  val materializeStatement: Parser[Operator] = (((MATERIALIZE ~ variableId) <~ INTO) ~ materializePath) <~ ";" ^^ { x =>
    val out_pat = x._2.path
    val par = OperatorParameters(Some(out_pat), List.empty)
    MaterializeOperator(x._1._1.pos, x._1._2, None, par)
  }

  val wrongStatement: Parser[Operator] =
    ((variableId <~ assignment_symbol) ~ ((WRONG <~ "(") ~ operator_parameters <~ ")")) ~
      (operatorInput | (onlyPath ^^ {
        (_, None)
      })) <~ ";" ^^ {

      x => {
        throw new CompilerException("At line " + x._1._2._1.pos.line + " : " + x._1._2._1.operator_type + " is not a valid operator")
        DifferenceOperator(x._1._2._1.pos, x._2._1, x._2._2, x._1._1, x._1._2._2)}
    }

  val meta_group_parameters: Parser[GroupMetaParameters] =
    ((metadata_attribute_list ^^ {
      MetaGroupByCondition(_)
    }).? ~ (";" ~> extend_aggfun_list).?) ^^ {
      x => GroupMetaParameters(x._1, x._2)
    }

  val region_group_parameters: Parser[GroupRegionParameters] =
    ((rep1sep(any_field_identifier, ",")).? ~ (";" ~> map_aggfun_list).?) ^^ {
      x => GroupRegionParameters(x._1, x._2)
    }

  val group_parameters: Parser[GroupParameters] = (meta_group_parameters.? ~ (";" ~> region_group_parameters).?) ^^ {
    x =>
      GroupParameters(x._1, x._2)
  }

  /*
  val groupStatement:Parser[Operator] =
    ((variableId <~ assignment_symbol) ~ ((GROUP <~ "(") ~ group_parameters <~ ")" )) ~ operatorInput <~ ";" ^^ {
      x=>
        val param = x._1._2._2

        GroupOperator(x._1._2._1.pos, x._2._1, x._2._2,x._1._1,param.meta_grouping, param.region_grouping)
    }

*/
  def remove_comments(query: String): String = {

    query.replaceAll("[#].*", "").replaceAll("\\s+$", "")
  }

  def phase1(query_raw: String): List[Operator] = {

    val query = remove_comments(query_raw)

    var remaining: Input = new CharSequenceReader(query)
    var failed = false
    var error_message: String = ""
    var result: List[Operator] = List.empty
    val validStatement = selectStatement |
      projectStatement |
      mergeStatement |
      joinStatement |
      extendStatement |
      orderStatement |
      coverStatement |
      summitStatement |
      histogramStatement |
      flatStatement |
      mapStatement |
      differenceStatement |
      unionStatement |
      materializeStatement |
      wrongStatement
    val filler = """(.|\n)*;""".r

    do {
      val outcome = parse(validStatement, remaining)
      outcome match {
        case Failure(_, s) => {
          failed = true
          error_message += "Invalid syntax at line " + s.pos.line +
            " at or after \"" +
            s.pos.longString.substring(s.pos.column - 1, Math.min(s.pos.column + 12, s.pos.longString.length - 1)) + "...\"\n"


          val filled = parse(filler, remaining)
          filled match {
            case s: Success[String] => remaining = filled.next
            case _ => throw new CompilerException("Missing \";\" character at the end of the query")
          }
        }
        case Success(r, s) => {
          result = result.:+(r.asInstanceOf[Operator])
          remaining = outcome.next
        }
      }
    } while (!remaining.atEnd)
    if (failed) throw new CompilerException(error_message)
    result
  }

  def phase2(list_statements: List[Operator]): Boolean = {

    var status: CompilerStatus = new CompilerStatus(server)

    var has_materialize : Boolean = false

    for (s <- list_statements) {
      s.check_input_number()
      s.check_input_variables(status)
      s.check_named_parameters()
      s match {
        case b: BuildingOperator2 => {
          val build_op = b.asInstanceOf[BuildingOperator2]
          build_op.check_output_variable(status)
          build_op.preprocess_operator(status)
          val new_var = build_op.translate_operator(status)
          status.add_variable(new_var.name, new_var.pos, new_var.payload)
        }
        case m: MaterializeOperator => {
          has_materialize = true
          m.translate_operator(status)
        }
      }
    }

    if (!has_materialize) throw new CompilerException("At list one MATERIALIZE statement is required in order to run the query")

    true

  }

}