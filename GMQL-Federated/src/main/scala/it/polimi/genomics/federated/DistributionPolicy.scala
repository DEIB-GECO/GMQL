package it.polimi.genomics.federated

import it.polimi.genomics.core.DAG.OperatorDAG
import it.polimi.genomics.core.DataStructures.{EXECUTED_ON, GMQLInstance, IROperator, IRStoreMD, IRStoreRD, LOCAL_INSTANCE, ReadOperator}
import org.slf4j.LoggerFactory


/** General trait for the specification of computation distribution policies
  *
  * The task of classes implementing the trait is to modify the input OperatorDAG
  * by adding/removing location annotation specifying where the specified operator
  * will be executed in the federation
  */
trait DistributionPolicy {
  def assignLocations(dag: OperatorDAG) : OperatorDAG
}

object StoreAtLocalDistributionPolicy extends DistributionPolicy {
  override def assignLocations(dag: OperatorDAG): OperatorDAG = {
    dag.roots.foreach {
      x => if(!x.hasExecutedOn) x.addAnnotation(EXECUTED_ON(LOCAL_INSTANCE))
    }
    dag
  }
}

object ProtectedPolicy extends DistributionPolicy {
  def decideLocation(op: IROperator):Unit = {
    if(op.isProtected){
      if(!op.hasExecutedOn){
        op.addAnnotation(EXECUTED_ON(LOCAL_INSTANCE))
      }
      else if (op.getExecutedOn != LOCAL_INSTANCE) {
        throw new GmqlFederatedException("Protected dataset cannot be moved to " + op.getExecutedOn)
      }
      op.getDependencies.foreach(decideLocation)
    }
  }

  override def assignLocations(dag: OperatorDAG): OperatorDAG = {
    dag.roots.foreach(decideLocation)
    dag
  }
}


/** Very simple location distribution policy based on the locality principle
  *
  * A node inherits the location of its parent if it has a single dependency, while
  * it picks a random (currently picks the first in the list) location from its
  * parents if it has more than one dependency.
  * In this way you delay as much as possible data movements.
  */
object DistributedPolicy extends DistributionPolicy {
  final val logger = LoggerFactory.getLogger(this.getClass)
  def getDependenciesLocations(deps: List[IROperator]): List[GMQLInstance] = {
    deps.map(decideLocation)
  }

  def decideLocation(op: IROperator): GMQLInstance = {
    val selLoc = {
      if(!op.hasExecutedOn){
        // the current operator does not have a location specification
        // we have to ask to the dependencies
        if(op.hasDependencies){
          val depLocs = getDependenciesLocations(op.getDependencies)
          val selectedLocation = depLocs.head //trivial policy
          op.addAnnotation(EXECUTED_ON(selectedLocation))
          selectedLocation
        } else {
          throw new IllegalStateException(s"[$op] Not possible to have a node without dependencies" +
            "and without location specification")
        }
      } else {
        if(op.hasDependencies)
          getDependenciesLocations(op.getDependencies)
        op.getExecutedOn
      }
    }
    logger.debug(op + "will be executed at " + selLoc)
    selLoc
  }
  override def assignLocations(dag: OperatorDAG): OperatorDAG = {
    dag.roots.foreach(decideLocation)
    dag
  }
}


