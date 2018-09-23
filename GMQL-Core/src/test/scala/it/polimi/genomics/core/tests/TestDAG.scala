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
    val changed = collection.mutable.Map[IROperator, IROperator]()
    var counter: Int = 0

    def checkSplitCondition(op: IROperator, dep: IROperator): Boolean =
      op.getExecutedOn != dep.getExecutedOn

    def getRelativeREAD(dep: IROperator, name: String): IROperator = {
      if(dep.isRegionOperator) IRReadFedRD(name)
      else if(dep.isMetaOperator) IRReadFedMD(name)
      else if(dep.isMetaJoinOperator) IRReadFedMetaJoin(name)
      else if(dep.isMetaGroupOperator) IRReadFedMetaGroup(name)
      else throw new IllegalArgumentException
    }

    def getRelativeSTORE(op: IROperator, name: String): IROperator = {
      if(op.isRegionOperator) IRStoreFedRD(op.asInstanceOf[RegionOperator], name)
      else if(op.isMetaOperator) IRStoreFedMD(op.asInstanceOf[MetaOperator], name)
      else if(op.isMetaJoinOperator) IRStoreFedMetaJoin(op.asInstanceOf[MetaJoinOperator], name)
      else if(op.isMetaGroupOperator) IRStoreFedMetaGroup(op.asInstanceOf[MetaGroupOperator], name)
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

            val newDep = {
              if(changed.contains(dep)) changed(dep)
              else {
                val res = getRelativeREAD(dep, "temp_" + counter)
                res.addAnnotation(EXECUTED_ON(op.getExecutedOn))

                val otherDAG = getRelativeSTORE(dep, "temp_" + counter)
                otherDAG.addAnnotation(EXECUTED_ON(dep.getExecutedOn))
                toBeExplored.push(otherDAG)
                changed += dep -> res
                counter += 1
                res
              }
            }
            newOp = newOp.substituteDependency(dep, newDep)
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


  val variableDAGFrame = new VariableDAGFrame(variableDAG, squeeze = true)
  showFrame(variableDAGFrame, "Variable DAG")
  val operatorDAGFrame = new OperatorDAGFrame(operatorDAG, squeeze = true)
  showFrame(operatorDAGFrame, "Operator DAG")


  val dagSplits = splitDAG(operatorDAG)
  dagSplits.foreach {
    case (instance, dag) => showFrame(new OperatorDAGFrame(dag, squeeze = true), instance.toString)
  }

}
