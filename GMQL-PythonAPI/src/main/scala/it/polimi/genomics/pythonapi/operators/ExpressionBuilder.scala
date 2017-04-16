package it.polimi.genomics.pythonapi.operators
import it.polimi.genomics.core.DataStructures._
import it.polimi.genomics.pythonapi.PythonManager
/**
  * Created by Luca Nanni on 11/04/17.
  * Email: luca.nanni@mail.polimi.it
  */

/**
  * This class enables the creation of complex region and meta conditions
  * based on the desires of the user
  * */
class ExpressionBuilder(index : Int) {

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
      case _ => RegionCondition.REG_OP.EQ
    }
  }

  def createRegionPredicate(name: String, operation: String, value : String): Unit =
  {
    val regOp = getRegionOperation(operation)
    // get the Variable
    val variable = PythonManager.getVariable(this.index)

    name match {
      case "chr" => RegionCondition.ChrCondition(value)
      case "right" => RegionCondition.RightEndCondition(regOp,value.toLong)
      case "left" => RegionCondition.LeftEndCondition(regOp, value.toLong)
      case "strand" => RegionCondition.StrandCondition(value)
      case "start" => RegionCondition.StartCondition(regOp, value.toLong)
      case "stop" => RegionCondition.StopCondition(regOp, value.toLong)
      case _ =>
        val field = variable.get_field_by_name(name)
        if(field.isDefined) {
          RegionCondition.Predicate(field.get,regOp, value)
        }
        else {
          RegionCondition.Predicate(_,_,_)
        }
    }
  }

  def createMetaPredicate(name : String, operation: String, value : String) = {
    val metaOp = getMetaOperation(operation)

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

  def createRegionBinaryCondition(pred1 : RegionCondition.RegionCondition, condition : String,
                                  pred2 : RegionCondition.RegionCondition) : RegionCondition.RegionCondition =
  {
    condition match {
      case "AND" => RegionCondition.AND(pred1, pred2)
      case "OR" => RegionCondition.OR(pred1, pred2)
    }
  }

  def createRegionUnaryCondition(pred: RegionCondition.RegionCondition,
                               condition: String): RegionCondition.RegionCondition =
  {
    condition match {
      case "NOT" => RegionCondition.NOT(pred)
    }
  }

}
