package it.polimi.genomics.pythonapi.operators
import it.polimi.genomics.core.DataStructures._
/**
  * Created by Luca Nanni on 11/04/17.
  * Email: luca.nanni@mail.polimi.it
  */

/**
  * This class enables the creation of complex region and meta conditions
  * based on the desires of the user
  * */
class ExpressionBuilder {

  def getMetaOperation(symbol : String) =
  {
    symbol match {
      case "GT" => MetadataCondition.META_OP.GT
      case "GTE" => MetadataCondition.META_OP.GTE
      case "LT" => MetadataCondition.META_OP.LT
      case "LTE" => MetadataCondition.META_OP.LTE
      case "EQ" => MetadataCondition.META_OP.EQ
      case "NOTEQ" => MetadataCondition.META_OP.NOTEQ
    }
  }

  def getRegionOperation(symbol : String) =
  {
    symbol match {
      case "GT" => RegionCondition.REG_OP.GT
      case "GTE" => RegionCondition.REG_OP.GTE
      case "LT" => RegionCondition.REG_OP.LT
      case "LTE" => RegionCondition.REG_OP.LTE
      case "EQ" => RegionCondition.REG_OP.EQ
      case "NOTEQ" => RegionCondition.REG_OP.NOTEQ
    }
  }

  def createMetaPredicate(name : String, operation: String, value : String) = {
    val metaOp = getMetaOperation(operation)
    // TEST
    MetadataCondition.Predicate(name, metaOp, value)
  }

  def createMetaBinaryCondition(pred1 : MetadataCondition.MetadataCondition, condition : String,
                                pred2 : MetadataCondition.MetadataCondition): MetadataCondition.MetadataCondition =
  {
    condition match {
      case "AND" => MetadataCondition.AND(pred1, pred2)
      case "OR" => MetadataCondition.OR(pred1, pred2)
    }
  }

  def createMetaUnaryCondition(pred: MetadataCondition.MetadataCondition,
                               condition: String): MetadataCondition.MetadataCondition =
  {
    condition match {
      case "NOT" => MetadataCondition.NOT(pred)
    }
  }

}
