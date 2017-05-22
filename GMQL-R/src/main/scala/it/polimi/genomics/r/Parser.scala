package it.polimi.genomics.r


import it.polimi.genomics.core.DataStructures.MetadataCondition.MetadataCondition
import it.polimi.genomics.compiler.GmqlParsers
import it.polimi.genomics.core.DataStructures.RegionCondition.RegionCondition


class PM extends GmqlParsers {

  def parsaMetadati(input: String): (String,Option[MetadataCondition]) = {

    println("input : " + input)
    val metadata = parse(meta_select_expr, input)

    metadata match {
      case Success(result,next) =>  ("OK",Some(result))
      case NoSuccess(result,next) =>  (result,None)
      case Error(result,next) =>  (result,None)
      case Failure(result,next) =>  (result,None)
    }
  }

  def parsaRegioni(input: String): (String, Option[RegionCondition]) = {

    println("input : " + input)
    val metadata = parse(region_select_expr, input)

    metadata match {
      case Success(result,next) =>  ("OK",Some(result))
      case NoSuccess(result,next) =>  (result,None)
      case Error(result,next) =>  (result,None)
      case Failure(result,next) =>  (result,None)
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