package it.polimi.genomics.core.tests

import it.polimi.genomics.core.DAG.{DAG, DAGFrame}
import it.polimi.genomics.core.DataStructures._
import scala.util.Random


object TestDAG extends App {

  val dag = TestQueries.query3

  val resIRVariables = new DAG(dag.toVariables(TestUtils.binning)).raw.toSet == dag.raw.toSet
  println(resIRVariables)


  def annotateDAGwithExecutionInstances(dag: DAG): DAG = {
    def randomAnnotator(op: IROperator): Unit = {
      // check if the annotation is already present
      if(!op.annotations.exists {case EXECUTED_ON(_) => true; case _ => false}) {
        val ann = op match {
          case IRReadRD(_, _, d) => EXECUTED_ON(d.instance)
          case IRReadMD(_, _, d) => EXECUTED_ON(d.instance)
          case _ => EXECUTED_ON(TestUtils.instances(new Random nextInt TestUtils.instances.length))
        }
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

  //val annotatedDAG = annotateDAGwithExecutionInstances(dag)



  val dagFrame = new DAGFrame(dag, squeeze = true)
  dagFrame.setSize(600, 620)
  dagFrame.setVisible(true)
}
