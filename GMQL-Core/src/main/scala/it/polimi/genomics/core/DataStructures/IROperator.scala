package it.polimi.genomics.core.DataStructures

import com.rits.cloning.Cloner
import it.polimi.genomics.core.DAG.DAGNode

/**
 * It represent a generic Intermediate Representation Dag operator
 */
abstract class IROperator extends Serializable with DAGNode[IROperator] {

  val operatorName: String = this.getClass.getSimpleName
    //.substring(this.getClass.getName.lastIndexOf('.')+1) + " " + this.hashCode()

  /** A list of the source datasets which are used by this operator */
  override def sources: Set[IRDataSet] = this.getDependencies.foldLeft(Set.empty[IRDataSet])((x, y) => x union y.sources)

  def hasExecutedOn: Boolean = this.annotations.exists({case EXECUTED_ON(_) => true})
  def getExecutedOn: GMQLInstance =
    if(this.hasExecutedOn) this.annotations.collect {
      case EXECUTED_ON(instance) => instance
    }.head else throw new IllegalAccessError

  /** Optional intermediate result stored to speed up computations */
  var intermediateResult : Option[AnyRef] = None

  /** Attributes for profiling */
  var requiresOutputProfile: Boolean = false
  var outputProfile: Option[GMQLDatasetProfile] = None

  /** DAG utilities for working with dependencies*/
  def getRegionDependencies: List[IROperator] = this.getDependencies.filter(p => p.isRegionOperator)
  def getMetaDependencies: List[IROperator] = this.getDependencies.filter(p => p.isMetaOperator)

  /*This is very bad software engineering...but it is impossible to do differently
  * due to the software architecture... :(*/
  def isRegionOperator: Boolean = false
  def isMetaOperator: Boolean = false
  def isMetaGroupOperator: Boolean = false
  def isMetaJoinOperator: Boolean = false

  override def toString: String = operatorName +
    //(if(sources.nonEmpty) "\n" + sources.mkString(",") else "") +
    (if(annotations.nonEmpty) "\n" + annotations.mkString(",") else "")

  def deepCopy: IROperator = {
    val cloner = new Cloner()
    cloner.deepClone(this)
  }
}

/** Indicates a IROperator which returns a metadata dataset */
abstract class MetaOperator extends IROperator {
  override def isMetaOperator: Boolean = true
}

/** Indicates a IROperator which returns a region dataset */
abstract class RegionOperator extends IROperator {
  override def isRegionOperator: Boolean = true
  var binSize : Option[Long] = None
}

/** Indicates a IROperator which returns the result of a meta-group operation */
abstract class MetaGroupOperator extends IROperator{
  override def isMetaGroupOperator: Boolean = true
}

/** Indicates a IROperator which returns the result of a meta-join operation*/
abstract class MetaJoinOperator extends IROperator{
  override def isMetaJoinOperator: Boolean = true
}

/** Structures for specifying an Optional MetaJoinOperator **/
abstract class OptionalMetaJoinOperator(operator:MetaJoinOperator) extends Serializable
{
  def getOperator : MetaJoinOperator = operator
}
case class SomeMetaJoinOperator(operator : MetaJoinOperator) extends OptionalMetaJoinOperator(operator)
case class NoMetaJoinOperator(operator : MetaJoinOperator) extends OptionalMetaJoinOperator(operator)
