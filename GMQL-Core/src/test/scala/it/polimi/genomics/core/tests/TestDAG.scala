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


  def generateExecutionDAGs(dagSplits: List[OperatorDAG]): MetaDAG = {
    val created = collection.mutable.Map[IROperator, ExecutionDAG]()

    def getOperatorDAGDependencies(ops: List[IROperator]): List[ExecutionDAG] = {

      def getDependenciesIds(op: IROperator): List[String] = {
        op match {
          case IRReadFedMD(name) => List(name)
          case IRReadFedRD(name) => List(name)
          case IRReadFedMetaGroup(name) => List(name)
          case IRReadFedMetaJoin(name) => List(name)
          case _ => if(op.hasDependencies) op.getDependencies.flatMap(getDependenciesIds) else Nil
        }
      }

      def getDependency(name: String): IROperator = dagSplits.flatMap {x => x.roots} filter {
        case op:IRStoreFedRD => op.name == name
        case op:IRStoreFedMD => op.name == name
        case op:IRStoreFedMetaJoin => op.name == name
        case op:IRStoreFedMetaGroup => op.name == name
        case _ => false
      } head

      def getExecutionDAG(op: IROperator): ExecutionDAG = {
        val opDeps = getDependenciesIds(op) map getDependency map {
          x => if(created.contains(x)) created(x) else {
            val res = getExecutionDAG(x)
            created += x -> res
            res
          }
        }
        new ExecutionDAG(List(new OperatorDAG(List(op))), opDeps)
      }

      if(ops.isEmpty) Nil
      else getExecutionDAG(ops.head) :: getOperatorDAGDependencies(ops.tail)
    }

    def mergeVariables(preRes: List[ExecutionDAG]): List[ExecutionDAG] = {
      val realStores = preRes filter {
        x => x.dag.head.roots.head match {
          case o:IRStoreRD => true
          case o:IRStoreMD => true
          case _ => false
        }
      }
      val newStores = realStores groupBy {
        x => {
          x.dag.head.roots.head match {
            case IRStoreMD(_,_, dataset) => dataset
            case IRStoreRD(_, _, _, _, dataset) => dataset
          }
        }
      } map {
        case (dataset, eDs) => new ExecutionDAG(eDs.flatMap(x => x.dag), eDs.flatMap(x => x.getDependencies).distinct)
      } toList

      newStores //::: preRes filter {x => !realStores.contains(x)}
    }

    val preRes = getOperatorDAGDependencies(dagSplits.flatMap {x => x.roots})

    new MetaDAG(mergeVariables(preRes))
  }

  val query = TestQueries.query4
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

  val executionDAGs = generateExecutionDAGs(dagSplits.values.toList)
  showFrame(new MetaDAGFrame(executionDAGs), title = "MetaDAG")
}
