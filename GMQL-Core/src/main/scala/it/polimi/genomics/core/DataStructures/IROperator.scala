package it.polimi.genomics.core.DataStructures

import it.polimi.genomics.core.DAG.DAGNode
import it.polimi.genomics.core.GMQLLoader

/**
  * It represent a generic Intermediate Representation Dag operator
  */
abstract class IROperator extends Serializable with DAGNode[IROperator] {

  val operatorName: String = this.getClass.getSimpleName
  //.substring(this.getClass.getName.lastIndexOf('.')+1) + " " + this.hashCode()

  /** A list of the source datasets which are used by this operator */
  override def sources: Set[IRDataSet] = this.getDependencies.foldLeft(Set.empty[IRDataSet])((x, y) => x union y.sources)

  def hasExecutedOn: Boolean = this.annotations.exists({
    case EXECUTED_ON(_) => true
    case _ => false
  })
  def getExecutedOn: GMQLInstance = this.annotations.collectFirst { case EXECUTED_ON(instance) => instance}.get

  /**
   * check if this or any of its ancestors is protected
   * @return
   */
  def isProtected: Boolean = this.annotations.exists({
    case PROTECTED => true
    case _ => false
  }) || this.getDependencies.foldLeft(false)((x, y) => x || y.isProtected)

  /** Optional intermediate result stored to speed up computations */
  var intermediateResult: Option[AnyRef] = None

  /** Attributes for profiling */
  var requiresOutputProfile: Boolean = false
  var outputProfile: Option[GMQLDatasetProfile] = None

  /** DAG utilities for working with dependencies */
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
    (if (annotations.nonEmpty) "\n" + annotations.mkString(",") else "")

  def deepCopy: IROperator = {
    val result = {
      if(this.hasDependencies) {
        var res = this
        val deps = res.getDependencies
        deps.foreach { d =>
          res = res.substituteDependency(d, d.deepCopy)
        }
        res
      } else {
        this match {
          case x:IRReadRD[_, _, _, _] => x.copy()
          case x:IRReadMD[_, _, _, _] => x.copy()
          case x:IRReadMEMMD => x.copy()
          case x:IRReadFedMD => x.copy()
          case x:IRReadFedMetaGroup => x.copy()
          case x:IRReadFedMetaJoin => x.copy()
          case x:IRReadFedRD => x.copy()
          case _ => throw new IllegalArgumentException("DeepCopy: Unknown operator")
        }
      }
    }
    result.annotations ++= this.annotations
    result.intermediateResult = this.intermediateResult
    result.requiresOutputProfile = this.requiresOutputProfile
    result.outputProfile = this.outputProfile

    result
  }
}

/** Indicates a IROperator which returns a metadata dataset */
abstract class MetaOperator extends IROperator {
  override def isMetaOperator: Boolean = true
}

/** Indicates a IROperator which returns a region dataset */
abstract class RegionOperator extends IROperator {
  override def isRegionOperator: Boolean = true

  var binSize: Option[Long] = None
}

/** Indicates a IROperator which returns the result of a meta-group operation */
abstract class MetaGroupOperator extends IROperator {
  override def isMetaGroupOperator: Boolean = true
}

/** Indicates a IROperator which returns the result of a meta-join operation */
abstract class MetaJoinOperator extends IROperator {
  override def isMetaJoinOperator: Boolean = true
}

/** Structures for specifying an Optional MetaJoinOperator **/
abstract class OptionalMetaJoinOperator(operator: MetaJoinOperator) extends Serializable {
  def getOperator: MetaJoinOperator = operator
}

case class SomeMetaJoinOperator(operator: MetaJoinOperator) extends OptionalMetaJoinOperator(operator)

case class NoMetaJoinOperator(operator: MetaJoinOperator) extends OptionalMetaJoinOperator(operator)

trait Federated {
  val name: String
  var path: Option[String]
}

trait ReadOperator {
  var paths: List[String]
  val loader: GMQLLoader[_, _, _, _]
  var dataset: IRDataSet

  def isProtected: Boolean
}