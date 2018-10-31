package it.polimi.genomics.federated.tests

import it.polimi.genomics.core.DAG.{DAGDraw, OperatorDAGFrame, VariableDAG}

object TestIROperator extends App{

  val query = TestQueries.query4

  val operatorDAG = new VariableDAG(query).toOperatorDAG
  DAGDraw.showFrame(new OperatorDAGFrame(operatorDAG, squeeze = true), "QueryDAG")

  val operatorDAGcopy = operatorDAG.copy
  DAGDraw.showFrame(new OperatorDAGFrame(operatorDAGcopy, squeeze = true), "QueryDAG_copy")
}
