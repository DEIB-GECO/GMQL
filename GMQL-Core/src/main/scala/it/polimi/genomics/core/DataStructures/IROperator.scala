package it.polimi.genomics.core.DataStructures

import scala.collection.mutable

/**
 * It represent a generic Intermediate Representation Daga operator
 */
class IROperator {
  val operatorName = this.getClass.getName.substring(this.getClass.getName.lastIndexOf('.')+1) + " " + this.hashCode()

  var intermediateResult : Option[AnyRef] = None

  override def toString = operatorName

  /**
    * Get the list of input operators for the current operator.
    * */
  def getOperatorList: List[IROperator] = {
    val result = mutable.Set[IROperator]()
    for (method <- this.getClass.getMethods) {
      if (method.getParameterCount == 0 && classOf[IROperator].isAssignableFrom(method.getReturnType)) {
        result.add(method.invoke(this).asInstanceOf[IROperator])
      }
    }
    result.toList
  }


}

/** Indicates a IROperator which returns a metadata dataset */
class MetaOperator extends IROperator

/** Indicates a IROperator which returns a region dataset */
class RegionOperator extends IROperator {
  var binSize : Option[Long] = None
}

/** Indicates a IROperator which returns the result of a meta-group operation */
class MetaGroupOperator extends IROperator

/** Indicates a IROperator which returns the result of a meta-join operation*/
class MetaJoinOperator extends IROperator


/** Structures for specifying an Optional MetaJoinOperator **/
abstract class OptionalMetaJoinOperator(operator:MetaJoinOperator) extends Serializable
{
  def getOperator : MetaJoinOperator = operator
}
case class SomeMetaJoinOperator(operator : MetaJoinOperator) extends OptionalMetaJoinOperator(operator)
case class NoMetaJoinOperator(operator : MetaJoinOperator) extends OptionalMetaJoinOperator(operator)
