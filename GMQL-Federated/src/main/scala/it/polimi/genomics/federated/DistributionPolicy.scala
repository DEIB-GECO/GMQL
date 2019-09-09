package it.polimi.genomics.federated

import it.polimi.genomics.core.DAG.OperatorDAG
import it.polimi.genomics.core.DataStructures.{EXECUTED_ON, GMQLInstance, IROperator, ReadOperator}
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

class ProtectedDistributionPolicy extends DistributionPolicy {
  override def assignLocations(dag: OperatorDAG): OperatorDAG = {

    def getDependenciesConstraints(deps: List[IROperator]): List[GMQLInstance] = {
      deps.map(decideLocation).filter(_.isDefined).map(_.get)
    }

    def decideLocation(op: IROperator): Option[GMQLInstance] = {
      val loc = op match {
        case operator: ReadOperator if operator.isProtected => Some(operator.getExecutedOn)
        case _ => {
          val depConstr: List[GMQLInstance] = if(op.hasDependencies)
            getDependenciesConstraints(op.getDependencies) else List()
          val constr = if(op.hasExecutedOn) op.getExecutedOn :: depConstr else depConstr
          val uniqueNames = constr.map(_.name).distinct
          if(uniqueNames.length > 1)
            throw new GmqlFederatedException("Impossible to allocate resources:\n" +
              s"Conflicting protection requirements (${uniqueNames.mkString(" VS ")})")
          else if(uniqueNames.length == 1)
            Some(constr.head)
          else
            None
          }
        }
      if(!op.hasExecutedOn && loc.isDefined)
        op.addAnnotation(EXECUTED_ON(loc.get))
      loc
    }
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
class LocalityDistributionPolicy extends DistributionPolicy {
  final val logger = LoggerFactory.getLogger(this.getClass)
  override def assignLocations(dag: OperatorDAG): OperatorDAG = {

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
            throw new IllegalStateException("Not possible to have a node without dependencies" +
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

    dag.roots.foreach(decideLocation)
    dag
  }
}


