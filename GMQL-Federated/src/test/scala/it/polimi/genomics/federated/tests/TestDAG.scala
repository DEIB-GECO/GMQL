package it.polimi.genomics.federated.tests

import it.polimi.genomics.core.DAG._
import it.polimi.genomics.federated.DAGManipulator


object TestDAG extends App {


  val query = TestQueries.queryPub
  val variableDAG = new VariableDAG(query)
  val operatorDAG = variableDAG.toOperatorDAG


  val variableDAGFrame = new VariableDAGFrame(variableDAG, squeeze = true)
  DAGDraw.showFrame(variableDAGFrame, "Variable DAG")
  val operatorDAGFrame = new OperatorDAGFrame(operatorDAG, squeeze = true)
  DAGDraw.showFrame(operatorDAGFrame, "Operator DAG")


  val dagSplits = DAGManipulator.splitDAG(operatorDAG, "jobId", "tempDir")
  dagSplits.foreach {
    case (instance, dag) => DAGDraw.showFrame(new OperatorDAGFrame(dag, squeeze = true), instance.toString)
  }

  val executionDAGs = DAGManipulator.generateExecutionDAGs(dagSplits.values.toList)
  DAGDraw.showFrame(new MetaDAGFrame(executionDAGs), title = "MetaDAG")
}
