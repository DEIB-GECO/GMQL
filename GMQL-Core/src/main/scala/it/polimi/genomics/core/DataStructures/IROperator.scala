package it.polimi.genomics.core.DataStructures

import it.polimi.genomics.core.DataStructures.OperatorType.OperatorType
import scala.collection.mutable

/**
 * It represent a generic Intermediate Representation Dag operator
 */
abstract class IROperator extends Serializable {
  val operatorName = this.getClass.getName.substring(this.getClass.getName.lastIndexOf('.')+1) + " " + this.hashCode()

  var intermediateResult : Option[AnyRef] = None
  override def toString = operatorName

  def getChildren: List[IROperator]
  def operatorType: OperatorType.OperatorType

  def left: Option[IROperator] = None
  def right: Option[IROperator] = None

  def isRegionOperator: Boolean = {
    this.operatorType == OperatorType.REGION_OPERATOR
  }

  def isMetaOperator: Boolean = {
    this.operatorType == OperatorType.META_OPERATOR
  }

  def getRegionChildren: List[IROperator] = this.getChildren.filter(p => p.isRegionOperator)
  def getMetaChildren: List[IROperator] = this.getChildren.filter(p => p.isMetaOperator)
}

/** Indicates a IROperator which returns a metadata dataset */
abstract class MetaOperator extends IROperator {
  override def operatorType: OperatorType = OperatorType.META_OPERATOR
}

/** Indicates a IROperator which returns a region dataset */
abstract class RegionOperator extends IROperator {
  var binSize : Option[Long] = None
  override def operatorType: OperatorType = OperatorType.REGION_OPERATOR
}

/** Indicates a IROperator which returns the result of a meta-group operation */
abstract class MetaGroupOperator extends IROperator{
  override def operatorType: OperatorType = OperatorType.META_GROUP_OPERATOR
}

/** Indicates a IROperator which returns the result of a meta-join operation*/
abstract class MetaJoinOperator extends IROperator{
  override def operatorType: OperatorType = OperatorType.META_JOIN_OPERATOR
}


/** Structures for specifying an Optional MetaJoinOperator **/
abstract class OptionalMetaJoinOperator(operator:MetaJoinOperator) extends Serializable
{
  def getOperator : MetaJoinOperator = operator
}
case class SomeMetaJoinOperator(operator : MetaJoinOperator) extends OptionalMetaJoinOperator(operator)
case class NoMetaJoinOperator(operator : MetaJoinOperator) extends OptionalMetaJoinOperator(operator)
