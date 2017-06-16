package it.polimi.genomics.compiler

import it.polimi.genomics.core.DataStructures.CoverParameters.CoverParam
import it.polimi.genomics.core.DataStructures.GroupMDParameters.Direction
import it.polimi.genomics.core.DataStructures.GroupMDParameters.Direction._
import it.polimi.genomics.core.DataStructures.JoinParametersRD.RegionBuilder
import it.polimi.genomics.core.DataStructures.JoinParametersRD.RegionBuilder._
import it.polimi.genomics.core.DataStructures.JoinParametersRD._
import it.polimi.genomics.core.DataStructures.MetaAggregate._
import it.polimi.genomics.core.DataStructures.MetaJoinCondition.{FullName, Exact, Default, AttributeEvaluationStrategy}
import it.polimi.genomics.core.DataStructures.MetadataCondition.{MetadataCondition, META_OP}
import it.polimi.genomics.core.DataStructures.MetadataCondition.META_OP._
import it.polimi.genomics.core.DataStructures.RegionAggregate._
import it.polimi.genomics.core.DataStructures.RegionCondition._
import it.polimi.genomics.core.DataStructures.RegionCondition.REG_OP._

import scala.util.parsing.combinator.JavaTokenParsers

/**
  * Created by pietro on 20/06/16.
  */
trait GmqlParsers extends JavaTokenParsers {

  override def stringLiteral:Parser[String] =
    ("\""+"""([^"\p{Cntrl}\\]|\\[\\'"bfnrt]|\\u[a-fA-F0-9]{4})*"""+"\"").r  |
    ("\'"+"""([^'\p{Cntrl}\\]|\\[\\'"bfnrt]|\\u[a-fA-F0-9]{4})*"""+"\'").r

  val variableId:Parser[VariableIdentifier] = positioned(ident ^^ {x => VariableIdentifier(x)})
  val variablePath:Parser[VariablePath] = positioned(
    ("[" ~> ident <~ "]") ~ ("""[A-Z|a-z|0-9/._:-]+""".r) ^^ {
      x => VariablePath(x._2,x._1)} )
  val onlyPath = """[A-Z|a-z|0-9._:-]*[/][A-Z|a-z|0-9/._:-]*""".r ^^ {x => VariablePath(x,null)}
  val materializePath = """[A-Z|a-z|0-9/._:-]+""".r ^^ {x => VariablePath(x,null)}
  val anyVariableIdentifier:Parser[Variable] = variableId | variablePath

  val fixed_field_position:Parser[Int] = (regex("[$](\\d|[1-9]\\d*)".r) ) ^^ {x => x.tail.toInt}
  val region_field_name:Parser[String] = rep1sep(ident, ".") ^^ {_.mkString(".")}
  val any_field_identifier:Parser[FieldPositionOrName] =
    fixed_field_position ^^ {FieldPosition(_)} |
      region_field_name ^^ {FieldName(_)}


  val field_value:Parser[Any] = (stringLiteral ^^ {x=> x.drop(1).dropRight(1)})  |
    (floatingPointNumber ^^ (_.toDouble))

  val ALLBUT:Parser[String] = """[a|A][l|L][l|L][b|B][u|U][t|T]""".r
  val IN:Parser[String] = """[i|I][n|N]""".r
  val AS:Parser[String] = """[a|A][s|S]""".r
  val OR:Parser[String] = """[o|O][r|R]""".r
  val AND:Parser[String] = """[a|A][n|N][d|D]""".r
  val NOT:Parser[String] = """[n|N][o|O][t|T]""".r
  val INTO:Parser[String] = """[i|I][n|N][t|T][o|O]""".r
  val LEFT:Parser[String] = """[l|L][e|E][f|F][t|T]""".r
  val RIGHT:Parser[String] = """[r|R][i|I][g|G][h|H][t|T]""".r
  val STAR:Parser[String] = "*"
  val MULT:Parser[String] = "*"
  val DIV:Parser[String] = "/"
  val SUM:Parser[String] = "+"
  val SUB:Parser[String] = "-"
  val STRAND:Parser[String] = """[s|S][t|T][r|R][a|A][n|N][d|D]""".r
  val CHR:Parser[String] = """[c|C][h|H][r|R]""".r |
    """[c|C][h|H][r|R][o|O][m|M]""".r |
    """[c|C][h|H][r|R][o|O][m|M][o|O][s|S][o|O][m|M][e|E]""".r
  val START:Parser[String] = """[s|S][t|T][a|A][r|R][t|T]""".r
  val STOP:Parser[String] = """[s|S][t|T][o|O][p|P]""".r
  val META:Parser[String] = """[m|M][e|E][t|T][a|A]""".r
  val UPSTREAM:Parser[String] =  """[u|U][p|P][s|S][t|T][r|R][e|E][a|A][m|M]""".r | """[u|U][p|P]""".r
  val DOWNSTREAM:Parser[String] = """[d|D][o|O][w|W][n|N][s|S][t|T][r|R][e|E][a|A][m|M]""".r | """[d|D][o|O][w|W][n|N]""".r
  val MINDIST:Parser[String] = """[m|M][i|I][n|N][d|D][i|I][s|S][t|T][a|A][n|N][c|C][e|E]""".r |
    """[m|M][i|I][n|N][d|D][i|I][s|S][t|T]""".r |
    """[m|M][d|D]""".r
  val DISTANCE:Parser[String] = """[d|D][i|I][s|S][t|T][a|A][n|N][c|C][e|E]""".r |
    """[d|D][i|I][s|S][t|T]""".r
  val DISTLESS:Parser[String] = """[d|D][l|L][e|E]""".r
  val DISTGREATER:Parser[String] = """[d|D][g|G][e|E]""".r
  val INTERSECT:Parser[String] = """[i|I][n|N][t|T][e|E][r|R][s|S][e|E][c|C][t|T]""".r |
    """[i|I][n|N][t|T]""".r
  val CONTIG:Parser[String] = """[c|C][o|O][n|N][t|T][i|I][g|G]""".r | """[c|C][a|A][t|T]""".r
  val ANY:Parser[String] = """[a|A][n|N][y|Y]""".r
  val ALL:Parser[String] = """[a|A][l|L][l|L]""".r
  val TOPG:Parser[String] = """[t|T][o|O][p|P][g|G]""".r
  val TOP:Parser[String] = """[t|T][o|O][p|P]""".r
  val ASC:Parser[String] = """[a|A][s|S][c|C]""".r
  val DESC:Parser[String] = """[d|D][e|E][s|S][c|C]""".r
  val EXACT:Parser[String] = """[e|E][x|X][a|A][c|C][t|T]""".r
  val FULLNAME:Parser[String] = """[f|F][u|U][l|L][l|L][n|N][a|A][m|M][e|E]""".r
  val TRUE:Parser[String] = """[t|T][r|R][u|U][e|E]""".r
  val FALSE:Parser[String] = """[f|F][a|A][l|L][s|S][e|E]""".r

  val region_field_name_with_wildcards:Parser[String] =
    (
      (rep1(ident <~ ".") ~ "?" ~ rep("."~>ident)) ^^ {
        x => x._1._1 ++ x._1._2 ++ x._2
      } |
      (rep(ident <~ ".") ~ "?" ~ rep1("."~>ident)) ^^ {
        x => x._1._1 ++ x._1._2 ++ x._2
      }
      ) ^^ { x=> x.mkString(".")}


  val metadata_attribute:Parser[String] = rep1sep(ident, ".") ^^ {_.mkString(".")}
  val metadata_attribute_list:Parser[List[String]] = rep1sep(metadata_attribute, ",")

  val rich_metadata_attribute:Parser[AttributeEvaluationStrategy] =
      ((EXACT ~> "(") ~> metadata_attribute <~ ")") ^^ {Exact(_)} |
        ((FULLNAME ~> "(") ~> metadata_attribute <~ ")") ^^ {FullName(_)} |
        metadata_attribute ^^ {Default(_)}

  val rich_metadata_attribute_list:Parser[List[AttributeEvaluationStrategy]] = rep1sep(rich_metadata_attribute, ",")


  val meta_value:Parser[String] = floatingPointNumber |
    (stringLiteral ^^ {
      x=>
        if (x.startsWith("'")){
          x.drop(1).dropRight(1).replace("\\'","'")
        } else {
          x.drop(1).dropRight(1).replace("\\\"","\"")
        }
    })

  val meta_operator:Parser[META_OP] = ("==" | "!="  | ">=" | "<=" | ">" | "<") ^^ {
    case "==" => META_OP.EQ
    case "!=" => META_OP.NOTEQ
    case ">" => META_OP.GT
    case "<" => META_OP.LT
    case "<=" => META_OP.LTE
    case ">=" => META_OP.GTE
  }
  val region_operator:Parser[REG_OP] = ("==" | "!=" | ">=" | "<=" | ">" | "<" ) ^^ {
    case "==" => REG_OP.EQ
    case "!=" => REG_OP.NOTEQ
    case ">" => REG_OP.GT
    case "<" => REG_OP.LT
    case "<=" => REG_OP.LTE
    case ">=" => REG_OP.GTE
  }

  val meta_select_single_condition:Parser[MetadataCondition] = (metadata_attribute ~ meta_operator ~ meta_value) ^^
    {x => it.polimi.genomics.core.DataStructures.MetadataCondition.Predicate(x._1._1, x._1._2, x._2)}

  lazy val meta_select_term:Parser[MetadataCondition] = meta_select_factor ~ ((AND ~> meta_select_factor)*) ^^ { x=>
    val left_most = x._1
    val list_others = x._2

    if (list_others.isEmpty) {
      left_most
    }
    else {
      (List(left_most) ++ list_others).reduce((x,y) =>
        it.polimi.genomics.core.DataStructures.MetadataCondition.AND(x,y))
    }
  }

  lazy val meta_select_expr:Parser[MetadataCondition] = (meta_select_term ~ ((OR ~> meta_select_term)*) ^^ { x=>
    val left_most = x._1
    val list_others = x._2

    if (list_others.isEmpty) {
      left_most
    }
    else {
      (List(left_most) ++ list_others).reduce((x,y) => it.polimi.genomics.core.DataStructures.MetadataCondition.OR(x,y))
    }
  })

  val meta_select_factor:Parser[MetadataCondition] = meta_select_single_condition |
    "(" ~> meta_select_expr <~ ")" |
    (((NOT ~ "(" )~> meta_select_expr <~ ")") ^^ (x=>it.polimi.genomics.core.DataStructures.MetadataCondition.NOT(x)))


  val select_sj_condition:Parser[MetaJoinConditionTemp] =
    (rich_metadata_attribute_list <~ IN)~variableId  ^^ {
      x => MetaJoinConditionTemp(x._1, x._2)
    }

  val region_cond_field_value:Parser[Any] = field_value |
    META ~> "(" ~> metadata_attribute <~ ")" ^^ {MetaAccessor(_)}

  val region_select_single_condition:Parser[RegionCondition] =
    (region_field_name ~ region_operator ~ region_cond_field_value) ^^
    {x => RegionPredicateTemp(x._1._1, x._1._2, x._2)}

  val region_select_single_condition_fixed:Parser[RegionCondition] =
    (fixed_field_position ~ region_operator ~ region_cond_field_value) ^^
    {x => it.polimi.genomics.core.DataStructures.RegionCondition.Predicate(x._1._1, x._1._2, x._2)}

  val region_strand_condition:Parser[StrandCondition] =
    STRAND ~> "==" ~> (
      ("""[\\+]""".r) |
      ("""[\\-]""".r) |
      ("""[\\*]""".r) |
      ("'+'" ^^ {_ => "+"}) |
      ("'-'" ^^ {_ => "-"}) |
      ("'*'" ^^ {_ => "*"}) |
      (("\""+"""[+]"""+"\"").r  ^^ {_ => "+"}) |
      ("\"-\"" ^^ {_ => "-"}) |
      ("\"*\"" ^^ {_ => "*"})
    ) ^^ {
    x =>
      it.polimi.genomics.core.DataStructures.RegionCondition.StrandCondition(x)
  }
  val region_chr_condition:Parser[ChrCondition] = (CHR ~> "==" ~> """[a-zA-Z0-9_\\*\\+\\-]+""".r) ^^
    {x=> it.polimi.genomics.core.DataStructures.RegionCondition.ChrCondition(x)}
  val region_left_condition:Parser[LeftEndCondition] = (LEFT ~> (region_operator ~ decimalNumber)) ^^
    {x=> it.polimi.genomics.core.DataStructures.RegionCondition.LeftEndCondition(x._1,x._2.toLong)}
  val region_right_condition:Parser[RightEndCondition] = (RIGHT ~> (region_operator ~ decimalNumber)) ^^
    {x=> it.polimi.genomics.core.DataStructures.RegionCondition.RightEndCondition(x._1,x._2.toLong)}
  val region_start_condition:Parser[StartCondition] = (START ~> (region_operator ~ decimalNumber)) ^^
    {x=> it.polimi.genomics.core.DataStructures.RegionCondition.StartCondition(x._1,x._2.toLong)}
  val region_stop_condition:Parser[StopCondition] = (STOP ~> (region_operator ~ decimalNumber)) ^^
    {x=> it.polimi.genomics.core.DataStructures.RegionCondition.StopCondition(x._1,x._2.toLong)}
  val region_coordinate_condition:Parser[RegionCondition] =
    region_strand_condition |
      region_chr_condition |
      region_left_condition |
      region_right_condition |
      region_stop_condition |
      region_start_condition

  lazy val region_select_term:Parser[RegionCondition] = region_select_factor ~ ((AND ~> region_select_factor)*) ^^ { x=>
    val left_most = x._1
    val list_others = x._2

    if (list_others.isEmpty) {
      left_most
    }
    else {
      (List(left_most) ++ list_others).reduce((x,y) => it.polimi.genomics.core.DataStructures.RegionCondition.AND(x,y))
    }
  }

  lazy val region_select_expr:Parser[RegionCondition] = region_select_term ~ ((OR ~> region_select_term)*) ^^ { x=>
    val left_most = x._1
    val list_others = x._2

    if (list_others.isEmpty) {
      left_most
    }
    else {
      (List(left_most) ++ list_others).reduce((x,y) => it.polimi.genomics.core.DataStructures.RegionCondition.OR(x,y))
    }
  }

  val region_select_factor:Parser[RegionCondition] =
    region_coordinate_condition |
    region_select_single_condition_fixed |
    region_select_single_condition |
    "(" ~> region_select_expr <~ ")" |
    (((NOT ~ "(" )~> region_select_expr <~ ")") ^^
      {x=>it.polimi.genomics.core.DataStructures.RegionCondition.NOT(x)})

  val allBut:Parser[Either[AllBut,List[SingleProjectOnRegion]]] =
    ALLBUT ~>
      rep1sep(
        region_field_name_with_wildcards ^^ {FieldNameWithWildCards(_)} |
        fixed_field_position ^^ {FieldPosition(_)} |
        region_field_name ^^ {FieldName(_)}
        , ",") ^^
      {
        x =>
          Left(AllBut(x))
      }


  val single_region_project:Parser[SingleProjectOnRegion] =
    region_field_name_with_wildcards ^^ {x => RegionProject(FieldNameWithWildCards(x))} |
      fixed_field_position ^^ {x => RegionProject(FieldPosition(x))} |
      region_field_name ^^ {x => RegionProject(FieldName(x))} 

  val region_project_list:Parser[Either[AllBut,List[SingleProjectOnRegion]]] =
    rep1sep(single_region_project, ",") ^^ {x => Right(x)}

  val region_project_cond:Parser[Either[AllBut,List[SingleProjectOnRegion]]] =
      allBut |
      region_project_list

  lazy val re_factor:Parser[RENode] = START ^^ {x=>RESTART()} |
    STOP ^^ {x => RESTOP()} | LEFT ^^ {x => RELEFT()} |
    RIGHT ^^ {x=> RERIGHT()} | CHR ^^ {x => RECHR()} | STRAND ^^ {x => RESTRAND()} |
    decimalNumber ^^ {x => REFloat(x.toDouble)} |
    any_field_identifier ^^ {x => REFieldNameOrPosition(x)} | "(" ~> re_expr <~ ")" |
    SUB ~> re_expr ^^ {x => RENegate(x)}

  val re_term: Parser[RENode] = re_factor ~ rep((MULT|DIV) ~ re_factor) ^^ {
    case t ~ ts => ts.foldLeft(t) {
      case (t1, "*" ~ t2) => REMUL(t1, t2)
      case (t1, "/" ~ t2) => REDIV(t1, t2)
    }
  }

  val re_expr: Parser[RENode] =
    re_term ~ rep((SUM|SUB) ~ re_term) ^^ {
    case t ~ ts => ts.foldLeft(t) {
      case (t1, "+" ~ t2) => READD(t1, t2)
      case (t1, "-" ~ t2) => RESUB(t1, t2)
    }
  }

  val single_region_modifier:Parser[SingleProjectOnRegion] =
    ((region_field_name ^^ {FieldName(_)}) <~ AS) ~ stringLiteral ^^ {
      x => RegionModifier(x._1, REStringConstant(x._2))
    } |
    (
      (
        not(RIGHT | LEFT | START | STOP) ~> any_field_identifier |
        RIGHT ^^ {x => FieldPosition(COORD_POS.RIGHT_POS)} |
        LEFT ^^ {x => FieldPosition(COORD_POS.LEFT_POS)} |
        START ^^ {x => FieldPosition(COORD_POS.START_POS)} |
        STOP ^^ {x => FieldPosition(COORD_POS.STOP_POS)}
        ) <~ AS
      ) ~ re_expr ^^ { x => RegionModifier(x._1,x._2)}

  val region_modifier_list:Parser[List[SingleProjectOnRegion]] = rep1sep(single_region_modifier, ",")


  lazy val me_factor:Parser[MENode] =
    decimalNumber ^^ {x => MEFloat(x.toDouble)} |
    metadata_attribute ^^ {x => MEName(x)} | "(" ~> me_expr <~ ")" |
    SUB ~> me_expr ^^ {x => MENegate(x)}

  val me_term: Parser[MENode] = me_factor ~ rep((MULT|DIV) ~ me_factor) ^^ {
    case t ~ ts => ts.foldLeft(t) {
      case (t1, "*" ~ t2) => MEMUL(t1, t2)
      case (t1, "/" ~ t2) => MEDIV(t1, t2)
    }
  }

  val me_expr: Parser[MENode] =
    me_term ~ rep((SUM|SUB) ~ me_term) ^^ {
      case t ~ ts => ts.foldLeft(t) {
        case (t1, "+" ~ t2) => MEADD(t1, t2)
        case (t1, "-" ~ t2) => MESUB(t1, t2)
      }
    }

  val single_metadata_modifier:Parser[MetaModifier] =
    (metadata_attribute <~ AS) ~ stringLiteral ^^ {
      x => MetaModifier(x._1, MEStringConstant(x._2))
    } |
      (
        (
          metadata_attribute
          ) <~ AS
        ) ~ me_expr ^^ { x => MetaModifier(x._1,x._2)}





  val single_meta_project:Parser[SingleProjectOnMeta] = metadata_attribute ^^ {MetaProject(_)}
  val meta_project_list:Parser[List[SingleProjectOnMeta]] = rep1sep(single_meta_project, ",")

  val join_up:Parser[AtomicCondition] = UPSTREAM ^^ {x=> Upstream()}
  val join_down:Parser[AtomicCondition] = DOWNSTREAM ^^ {x=> DownStream()}
  val join_midistance:Parser[AtomicCondition] = MINDIST ~> "(" ~> wholeNumber <~ ")" ^^
    {x => MinDistance(x.toInt)}
  val join_distless:Parser[AtomicCondition] =
    ((DISTANCE ~> ("<=" | "<") ~> ("-" ~> wholeNumber) |
      DISTLESS ~> "(" ~> ("-" ~> wholeNumber)  <~ ")") ^^ {x => DistLess(-x.toLong)}) |
      ((DISTANCE ~> ("<=" | "<") ~> wholeNumber |
    DISTLESS ~> "(" ~> wholeNumber <~ ")") ^^ {x => DistLess(x.toLong)})
  val join_distgreater:Parser[AtomicCondition] = (DISTANCE ~> ( ">=" | ">") ~> wholeNumber |
    DISTGREATER ~> "(" ~> wholeNumber <~ ")") ^^ {x => DistGreater(x.toLong)}
  val join_atomic_condition:Parser[AtomicCondition] = join_up | join_down |
    join_distgreater | join_distless | join_midistance

  val join_region_condition:Parser[List[AtomicCondition]] = rep1sep(join_atomic_condition, ",")

  val region_builder:Parser[RegionBuilder] = LEFT ^^ {x => RegionBuilder.LEFT} |
    RIGHT ^^ {x => RegionBuilder.RIGHT} |
    INTERSECT ^^ {x => RegionBuilder.INTERSECTION} |
    CONTIG ^^ {x => RegionBuilder.CONTIG}

  val extend_aggfun:Parser[RegionsToMetaTemp] =
    ((metadata_attribute <~ AS)  ~ (ident <~ ("(" ~ ")"))) ^^ {
      x => RegionsToMetaTemp(x._2, None, Some(x._1))
    } |
    (metadata_attribute <~ AS)  ~ ((ident <~ "(") ~ (any_field_identifier <~ ")")) ^^ {
      x => RegionsToMetaTemp(x._2._1,Some(x._2._2),Some(x._1))
    }

  val extend_aggfun_list:Parser[List[RegionsToMetaTemp]] = rep1sep(extend_aggfun, ",")

  val metadata_order_single:Parser[(String,Direction)] =
    (metadata_attribute<~ ASC) ^^ {(_,Direction.ASC)} |
      (metadata_attribute<~ DESC) ^^ {(_,Direction.DESC)} |
      metadata_attribute ^^ {(_,Direction.ASC)}

  val meta_order_list:Parser[List[(String,Direction)]] = rep1sep(metadata_order_single, ",")

  val region_order_single:Parser[(FieldPositionOrName,Direction)] =
    (any_field_identifier<~ ASC) ^^ {(_,Direction.ASC)} |
      (any_field_identifier<~ DESC) ^^ {(_,Direction.DESC)} |
      any_field_identifier ^^ {(_,Direction.ASC)}

  val region_order_list:Parser[List[(FieldPositionOrName,Direction)]] = rep1sep(region_order_single, ",")

  val cover_param:Parser[CoverParam] =
    wholeNumber ^^ {x => new it.polimi.genomics.core.DataStructures.CoverParameters.N{override val n=x.toInt;}} |
      ANY ^^ {x=> new it.polimi.genomics.core.DataStructures.CoverParameters.ANY{}} |
      "("~>ALL ~> SUM ~> wholeNumber ~ (")" ~> DIV ~> wholeNumber) ^^ {
        x => new it.polimi.genomics.core.DataStructures.CoverParameters.ALL{
          override val fun = (a:Int) => (a + x._1.toInt) / x._2.toInt
        }
      } |
      ALL ~> DIV ~> wholeNumber ^^ {
        x => new it.polimi.genomics.core.DataStructures.CoverParameters.ALL{
          override val fun = (a:Int) => a  / x.toInt
        }
      } |
      ALL ^^ {x=> new it.polimi.genomics.core.DataStructures.CoverParameters.ALL{}}

  val cover_boundaries:Parser[(CoverParam,CoverParam)] = (cover_param <~ ",") ~ cover_param ^^ {
    x => (x._1,x._2)
  }

  val map_aggfun:Parser[RegionsToRegionTemp] =
    (((region_field_name ^^ {FieldName(_)}) <~ AS).? ~ ((ident <~ ("(" ~ ")"))) ^^ {
      x => RegionsToRegionTemp(x._2,
        None,
        if(x._1.isDefined)  x._1 else Some(FieldName(x._2.toLowerCase())))
    }) |
  (((region_field_name ^^ {FieldName(_)}) <~ AS).? ~ ((ident <~ "(") ~ (any_field_identifier <~ ")")) ^^ {
      x => RegionsToRegionTemp(x._2._1,
        Some(x._2._2),
        if(x._1.isDefined)  x._1 else Some(FieldName(x._2._1.toLowerCase())))
    })

  val map_aggfun_list:Parser[List[RegionsToRegionTemp]] = repsep(map_aggfun, ",")


  val difference_type:Parser[Boolean] = TRUE ^^ {x => true} | FALSE ^^ {x => false}

}
