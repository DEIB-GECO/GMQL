package it.polimi.genomics.core.DAG

import it.polimi.genomics.core.DataStructures.ExecutionParameters.BinningParameter
import it.polimi.genomics.core.DataStructures._

class ExecutionDAG(val dag: List[OperatorDAG], deps: List[ExecutionDAG]) extends DAGNode[ExecutionDAG] {
  /** Returns the list of dependencies of the node */
  override def getDependencies: List[ExecutionDAG] = this.deps

  private def getFederatedDSources(op: IROperator): Set[String] = {

    var res = Set.empty[String]


    val tempRes = op match {
      case IRReadFedMD(name, _) => Some(name )
      case IRReadFedMetaGroup(name, _) => Some(name)
      case IRReadFedMetaJoin(name, _) => Some(name)
      case IRReadFedRD(name, _) => Some(name)
      case _ => None
    }

    if(tempRes.isDefined) res += tempRes.get

    if (op.hasDependencies) {
      op.getDependencies.foreach { x =>
        res ++= getFederatedDSources(x)
      }

    }
    res
  }


  /** A list of the source datasets which are used by this node */
  def getFederatedsources: Set[String] = dag.flatMap(_.roots).flatMap(getFederatedDSources).toSet

  override def substituteDependency(oldDep: ExecutionDAG, newDep: ExecutionDAG): ExecutionDAG =
    throw new NotImplementedError()

  def where = dag.head.roots.head.getExecutedOn

  def toIRVariable(implicit binning: BinningParameter) = {
    new OperatorDAG(dag.flatMap { x =>
      x.roots
    }).toVariables
  }

  /** A list of the source datasets which are used by this node */
  override def sources: Set[IRDataSet] = ???
}