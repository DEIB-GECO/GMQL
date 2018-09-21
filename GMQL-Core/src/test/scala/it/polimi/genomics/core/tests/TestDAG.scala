package it.polimi.genomics.core.tests

import it.polimi.genomics.core.DAG._
import it.polimi.genomics.core.DataStructures._
import scala.util.Random

object TestDAG extends App {

  def showFrame[T <: DAGNode[T]](dagFrame: DAGFrame[T], title: String): Unit = {
    dagFrame.setSize(1000, 600)
    dagFrame.setVisible(true)
    dagFrame.setTitle(title)
  }

  def annotateOperatorDAG(dag: OperatorDAG): Unit = {
    def annotateOperator(op: IROperator): Unit = {
      if(!op.hasExecutedOn)
        op.addAnnotation(EXECUTED_ON(Random.shuffle(TestUtils.instances).head))
    }
    def rec(op: IROperator): Unit = {
      annotateOperator(op)
      if(op.getDependencies.nonEmpty)
        op.getDependencies.foreach(rec)
    }
    dag.roots.foreach(rec)
  }

  def splitDAG(dag: OperatorDAG): Map[GMQLInstance, OperatorDAG] = {
    val toBeExplored = collection.mutable.Stack[IROperator]()

    def checkSplitCondition(op: IROperator, dep: IROperator): Boolean =
      op.getExecutedOn != dep.getExecutedOn

    def getRelativeREAD(dep: IROperator): IROperator = {
      if(dep.isRegionOperator) IRReadFedRD("bla")
      else if(dep.isMetaOperator) IRReadFedMD("bla")
      else if(dep.isMetaJoinOperator) IRReadFedMetaJoin("bla")
      else if(dep.isMetaGroupOperator) IRReadFedMetaGroup("bla")
      else throw new IllegalArgumentException
    }

    def getRelativeSTORE(op: IROperator): IROperator = {
      if(op.isRegionOperator) IRStoreFedRD(op.asInstanceOf[RegionOperator], "bla")
      else if(op.isMetaOperator) IRStoreFedMD(op.asInstanceOf[MetaOperator], "bla")
      else if(op.isMetaJoinOperator) IRStoreFedMetaJoin(op.asInstanceOf[MetaJoinOperator],"bla")
      else if(op.isMetaGroupOperator) IRStoreFedMetaGroup(op.asInstanceOf[MetaGroupOperator], "bla")
      else throw new IllegalArgumentException
    }

    def changeOperator(op: IROperator): IROperator = {
      if(!op.hasDependencies) op
      else {
        val opDeps = op.getDependencies
        var newOp = op
        val oldAnnotations = op.annotations
        opDeps.foreach { dep =>
          if(checkSplitCondition(newOp, dep)){
            val newDep = getRelativeREAD(dep)
            newDep.addAnnotation(EXECUTED_ON(op.getExecutedOn))
            newOp = newOp.substituteDependency(dep, newDep)

            val otherDAG = getRelativeSTORE(dep)
            otherDAG.addAnnotation(EXECUTED_ON(dep.getExecutedOn))
            toBeExplored.push(otherDAG)
          }
          else newOp = newOp.substituteDependency(dep, changeOperator(dep))
          newOp.annotations ++= oldAnnotations
        }
        newOp
      }
    }

    def emptyStack(): List[IROperator] = {
      if(toBeExplored.isEmpty) Nil
      else changeOperator(toBeExplored.pop()) :: emptyStack()
    }

    (dag.roots.map(changeOperator) ::: emptyStack())
      .groupBy(x => x.getExecutedOn).map {
      case (instance, ops) => instance -> new OperatorDAG(ops)
    }
  }

  val query = TestQueries.query3
  val variableDAG = new VariableDAG(query)
  val operatorDAG = variableDAG.toOperatorDAG


//  val variableDAGFrame = new VariableDAGFrame(variableDAG, squeeze = true)
//  showFrame(variableDAGFrame, "Variable DAG")
  val operatorDAGFrame = new OperatorDAGFrame(operatorDAG, squeeze = true)
  showFrame(operatorDAGFrame, "Operator DAG")


  val dagSplits = splitDAG(operatorDAG)
  dagSplits.foreach {
    case (instance, dag) => showFrame(new OperatorDAGFrame(dag, squeeze = true), instance.toString)
  }

}
