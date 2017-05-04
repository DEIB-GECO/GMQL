package it.polimi.genomics.r


import it.polimi.genomics.core.DataStructures.MetadataCondition.MetadataCondition
import it.polimi.genomics.compiler.GmqlParsers
import it.polimi.genomics.core.DataStructures.RegionCondition.RegionCondition


class PM extends GmqlParsers {

  def parsaMetadati(input: String): (String,MetadataCondition) = {

    println("input : " + input)
    val metadata = parse(meta_select_expr, input)

    metadata match {
      case Success(result,next) => return ("OK",result)
      case NoSuccess(result,next) => return (result,null)
      case Error(result,next) => return (result,null)
      case Failure(result,next) => return (result,null)
    }
  }

  def parsaRegioni(input: String): (String,RegionCondition) = {

    println("input : " + input)
    val metadata = parse(region_select_expr, input)

    metadata match {
      case Success(result,next) => return ("OK",result)
      case NoSuccess(result,next) => return (result,null)
      case Error(result,next) => return (result,null)
      case Failure(result,next) => return (result,null)
    }
   }

}
object Parser
{
  def main(args: Array[String])
  {
    val t = new PM()
    val input = "NOT(asa<6 AND NOT(asa<6) AND sadas== 'dasd' " +
      "AND asdas==6 AND (adasd=='adsd' AND (a<6 OR b>4) OR c=='c'))"

    t.parsaMetadati(input)

  }

}