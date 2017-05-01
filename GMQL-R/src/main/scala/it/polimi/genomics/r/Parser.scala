package it.polimi.genomics.r


import scala.util.parsing.combinator._
import it.polimi.genomics.core.DataStructures
import it.polimi.genomics.core.DataStructures.MetadataCondition.META_OP.META_OP
import it.polimi.genomics.core.DataStructures.MetadataCondition.{META_OP, MetadataCondition}
import it.polimi.genomics.core.DataStructures.RegionCondition.REG_OP
import it.polimi.genomics.core.DataStructures.RegionCondition.REG_OP.REG_OP

class PM extends JavaTokenParsers
{
  val meta_operator:Parser[META_OP] = ("==" | "!="  | ">=" | "<=" | ">" | "<") ^^ {
    case "==" => META_OP.EQ
    case "!=" => META_OP.NOTEQ
    case ">" => META_OP.GT
    case "<" => META_OP.LT
    case "<=" => META_OP.LTE
    case ">=" => META_OP.GTE
  }

  val logic_operator:Parser[String] ="AND" |
    "OR" |
    "NOT"

  val attr_filed: Parser[String] = """[a-zA-Z_]\w*""".r
  val value: Parser[String] = """'[a-zA-Z_]\w*'""".r | floatingPointNumber
  val atomic: Parser[MetadataCondition] = attr_filed ~ meta_operator ~ value ^^
    {x => DataStructures.MetadataCondition.Predicate(x._1._1, x._1._2, x._2)}
  val pred: Parser[Any] = rep1("(" ~ pred ~ ")" | atomic | logic_operator)

  val axiom: Parser[Any] = "NOT" ~ pred | pred


  def parsa(input: String): Unit = {

    println("input : " + input)
    println(parse(axiom, input))

  }

}
object Parser
{
  def main(args: Array[String])
  {
    val t = new PM()
    val input = "NOT(asa<6 AND NOT(asa<6) AND sadas== 'dasd' " +
      "OR asdas==6 AND (adasd=='adsd' AND (a<6 OR b>4) OR c=='c'))"

    t.parsa(input)

  }

}