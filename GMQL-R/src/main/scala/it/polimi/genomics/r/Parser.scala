package it.polimi.genomics.r


import it.polimi.genomics.core.DataStructures.MetadataCondition.MetadataCondition
import it.polimi.genomics.compiler.GmqlParsers
import it.polimi.genomics.compiler.ProjectOperator
import it.polimi.genomics.core.DataStructures.RegionAggregate.RegionFunction
import it.polimi.genomics.core.DataStructures.RegionCondition.RegionCondition


class Parser extends GmqlParsers {

  def parseSelectMetadata(input: String): (String,Option[MetadataCondition]) = {

    //println("input : " + input)
    val metadata = parse(meta_select_expr, input)

    metadata match
    {
      case Success(result,next) =>  ("OK",Some(result))
      case NoSuccess(result,next) =>  (result,None)
      case Error(result,next) =>  (result,None)
      case Failure(result,next) =>  (result,None)
    }
  }

  def parseSelectRegions(input: String): (String, Option[RegionCondition]) = {

    //println("input : " + input)
    val metadata = parse(region_select_expr, input)

    metadata match
    {
      case Success(result,next) =>  ("OK",Some(result))
      case NoSuccess(result,next) =>  (result,None)
      case Error(result,next) =>  (result,None)
      case Failure(result,next) =>  (result,None)
    }
   }

  def parseProjectRegion(input: String): (String, Option[RegionCondition]) = {

    //println("input : " + input)
    val metadata = parse(region_select_expr, input)

    metadata match
    {
      case Success(result,next) =>  ("OK",Some(result))
      case NoSuccess(result,next) =>  (result,None)
      case Error(result,next) =>  (result,None)
      case Failure(result,next) =>  (result,None)
    }
  }

  def parseProjectMetadata(input: String): (String, Option[RegionCondition]) = {

    //println("input : " + input)
    val metadata = parse(region_select_expr, input)

    metadata match
    {
      case Success(result,next) =>  ("OK",Some(result))
      case NoSuccess(result,next) =>  (result,None)
      case Error(result,next) =>  (result,None)
      case Failure(result,next) =>  (result,None)
    }
  }
}