package it.polimi.genomics.core.tests

import it.polimi.genomics.core.DAG.{OperatorDAGFrame, VariableDAG, VariableDAGFrame, OperatorDAG}
import it.polimi.genomics.core.DataStructures._

import scala.util.Random


object TestDAG extends App {

  val query = TestQueries.query3
  //val dag = new DAG(query)
  val variableDAG = new VariableDAG(query)
  val operatorDAG = new OperatorDAG(query.flatMap(x => List(x.metaDag, x.regionDag)))

//  val resIRVariables = new DAG(dag.toVariables(TestUtils.binning)).raw.toSet == dag.raw.toSet
//  println(resIRVariables)


  def annotateDAGwithExecutionInstances(dag: VariableDAG): VariableDAG = {
    def randomAnnotator(op: IRVariable): Unit = {

      // get the instance from the source set of the operator
      val selectedInstance = op.sourceInstances.toList(new Random()
        .nextInt(op.sourceInstances.size))

      // check if the annotation is already present
      if(!op.annotations.exists {case EXECUTED_ON(_) => true; case _ => false}) {
        val ann = EXECUTED_ON(selectedInstance)
        op.addAnnotation(ann)
      }
      if(op.getDependencies.nonEmpty) op.getDependencies.foreach(randomAnnotator)
    }
    dag.roots.foreach(randomAnnotator)
    dag
  }

  def splitDAGBasedOnExecutionInstances(dag: VariableDAG): VariableDAG = {
    def getExecutedOnAnn(i: IRVariable): GMQLInstance = {
      i.annotations.collect {case EXECUTED_ON(instance) => instance} head
    }

    def checkCondition(xInstance: GMQLInstance, depInstances: List[GMQLInstance]): Boolean = {
      val depInstancesDistinct = depInstances.distinct
      (depInstancesDistinct.length == 1) && depInstancesDistinct.contains(xInstance)
    }

    dag.subDAG(x => {
      val xInstance = getExecutedOnAnn(x)
      val depInstances = x.getDependencies.map(getExecutedOnAnn)
      checkCondition(xInstance, depInstances)
    })
  }

    annotateDAGwithExecutionInstances(variableDAG)
    val splitDAG = splitDAGBasedOnExecutionInstances(variableDAG)
//  val dagFrame = new DAGFrame(dag, squeeze = true)
//  dagFrame.setSize(1000, 600)
//  dagFrame.setVisible(true)


  val variableDAGFrame = new VariableDAGFrame(variableDAG, squeeze = true)
  variableDAGFrame.setSize(1000, 600)
  variableDAGFrame.setVisible(true)

  val operatorDAGFrame = new OperatorDAGFrame(operatorDAG, squeeze = true)
  operatorDAGFrame.setSize(1000, 600)
  operatorDAGFrame.setVisible(true)

//  val variableDAGFrameSplit = new VariableDAGFrame(splitDAG, squeeze = true)
//  variableDAGFrameSplit.setSize(1000, 600)
//  variableDAGFrameSplit.setVisible(true)

}
