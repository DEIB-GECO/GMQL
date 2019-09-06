package it.polimi.genomics.federated.tests

import it.polimi.genomics.core.DAG.{DAGDraw, OperatorDAGFrame, VariableDAG, VariableDAGFrame}
import it.polimi.genomics.federated.{LocalityDistributionPolicy, ProtectedDistributionPolicy}

object TestLocalityPolicy extends App {

  val policy = new ProtectedDistributionPolicy()

  val query = TestQueries.queryLocationPolicy
  val variableDAG = new VariableDAG(query)
  val operatorDAG = variableDAG.toOperatorDAG

  val variableDAGFrame = new VariableDAGFrame(variableDAG, squeeze = true)
  DAGDraw.showFrame(variableDAGFrame, "Variable DAG")
  val operatorDAGFrame = new OperatorDAGFrame(operatorDAG, squeeze = true)
  DAGDraw.showFrame(operatorDAGFrame, "Operator DAG")

  policy.assignLocations(operatorDAG)
  val opDAGFrameWithLocations = new OperatorDAGFrame(operatorDAG, squeeze = true)
  DAGDraw.showFrame(opDAGFrameWithLocations, "Operator DAG with locations")
}
