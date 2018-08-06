package it.polimi.genomics.core.tests

import it.polimi.genomics.core.DAG.{DAG, DAGFrame, VariableDAG, VariableDAGFrame}
import it.polimi.genomics.core.DataStructures._

import scala.util.Random


object TestDAG extends App {

  val query = TestQueries.query3
  val dag = new DAG(query)
  val variableDAG = new VariableDAG(query)

  val resIRVariables = new DAG(dag.toVariables(TestUtils.binning)).raw.toSet == dag.raw.toSet
  println(resIRVariables)


  def annotateDAGwithExecutionInstances(dag: DAG): DAG = {
    def randomAnnotator(op: IROperator): Unit = {

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
    dag.raw.foreach(randomAnnotator)
    dag
  }

//  def splitDAGBasedOnExecutionInstances(dag: DAG): DAG = {
//    def splitPoint
//    dag.subDAG()
//  }

  annotateDAGwithExecutionInstances(dag)

  val dagFrame = new DAGFrame(dag, squeeze = true)
  dagFrame.setSize(1000, 600)
  dagFrame.setVisible(true)


  val variableDAGFrame = new VariableDAGFrame(variableDAG)
  variableDAGFrame.setSize(1000, 600)
  variableDAGFrame.setVisible(true)

}
