package it.polimi.genomics.core.DAG

import com.rits.cloning.Cloner
import it.polimi.genomics.core.DataStructures.ExecutionParameters.BinningParameter
import it.polimi.genomics.core.DataStructures._


trait DAGNode[T <: DAGNode[T]] {
  /** A list of annotations which can be attached to the operator */
  val annotations: collection.mutable.Set[OperatorAnnotation] = collection.mutable.Set()

  def addAnnotation(annotation: OperatorAnnotation): Unit = annotations += annotation
  def removeAnnotation(annotation: OperatorAnnotation): Unit = annotations.remove(annotation)

  /** Returns the list of dependencies of the node*/
  def getDependencies: List[T]

  def hasDependencies: Boolean = getDependencies.nonEmpty

  /** A list of the source datasets which are used by this node */
  def sources: Set[IRDataSet]
  /** Returns the set of GMQLInstance which the source datasets come from*/
  def sourceInstances: Set[GMQLInstance] = this.sources.map(_.instance)

  def substituteDependency(oldDep: T, newDep: T): T
}

class DependencyException(message: String = "Dependency not found!") extends Exception(message)

abstract class GenericDAG[T <: DAGNode[T], A <: GenericDAG[T, A]](val roots: List[T]) {
  private val depthWidth: collection.mutable.Map[Int, Int] = collection.mutable.Map[Int, Int]()

  def create(roots: List[T]): A

  def union( other : GenericDAG[T, A] ) : A  =
    create( this.roots.union( other.roots ).distinct )

  /** Returns a DAG in which all the nodes of the original DAG which
    * satisfy the given predicate become root nodes of the new DAG
    *
    * @param pred: a predicate on the IROperator
    * @return a new DAG
    */
  def subDAG(pred: T => Boolean): A = {
    def _subDAG(ops: List[T], pred:T => Boolean): List[T] = {
      if (ops.isEmpty) List[T]() else ops.filter(pred) ++ _subDAG(ops.flatMap(x => x.getDependencies), pred)
    }
    _subDAG(this.roots, pred).map(x => create(List(x))).reduce( (x,y) => x.union(y) )
  }

  // computes the depthWith Map, for each depth the number of nodes at that depth
  private def _depthWidth(root: T, depth: Int): Unit = {

    for ( child <- root.getDependencies ) {
      _depthWidth(child, depth+1)
    }
    depthWidth(depth) = if( depthWidth.contains(depth) ) depthWidth(depth)+1 else 1
  }

  /**
    *  Maximum number of elements having the same depth
    * @return
    */
  def maxWidth():Int = {
    if( depthWidth.isEmpty ) {
      this.roots.foreach(x => _depthWidth( x , 0 ))
    }
    depthWidth.values.max
    //  val depth = depthWidth.filter( x => x._2==max )
  }

  def widthAt(depth: Int): Int = {
    if( depthWidth.isEmpty ) {
      this.roots.foreach(x => _depthWidth( x , 0 ))
    }
    depthWidth(depth)

  }

  def depth(): Int = {
    if( depthWidth.isEmpty ) {
      this.roots.foreach(x => _depthWidth( x , 0 ))
    }
    depthWidth.keys.max
  }

}


class OperatorDAG(raw: List[IROperator]) extends GenericDAG[IROperator, OperatorDAG](raw) {

  def copy: OperatorDAG = {
    val cloner = new Cloner()
    cloner.registerImmutable(List[IROperator]().getClass)
    cloner.deepClone(this)
  }

  // For a DAG to be Variable-consistent it must not have any root node which is different from
  // IRStoreMD and IRStoreRD
  private def checkVariableConsistency: Boolean = !this.roots.exists {
    case IRStoreRD(_, _, _, _, _) | IRStoreMD(_, _, _) => false
    case _ => true
  }

  /**
    * Returns the DAG in the form of List of IRVariables
    * @param binning implicit parameter
    * @return a list of IRVariable
    */
  def toVariables(implicit binning: BinningParameter): List[IRVariable] = {
    this.getAssociations.map {case (m, r) => IRVariable(m, r, r.schema)(binning)}
  }


  def getAssociations: List[(IRStoreMD, IRStoreRD)] = {
    if(checkVariableConsistency) {
      def getMap(op: IROperator): (IRDataSet, IROperator) = {
        op match {
          case IRStoreMD(_, _, dataSet) => (dataSet, op)
          case IRStoreRD(_, _, _, _, dataSet) => (dataSet, op)
        }
      }

      // Grouping IRVariables together
      val outputDatasets: Map[IRDataSet, List[IROperator]] = this.roots.map(getMap)
        .groupBy(_._1).mapValues(_.map(x => x._2))

      def getDagParts(l: List[IROperator]): (IRStoreMD, IRStoreRD) = {
        if (l.length != 2)
          throw new IllegalStateException("A dataset is an output of multiple IRVariables!")
        else {
          val metaDag = l.filter { case IRStoreMD(_, _, _) => true; case _ => false }.head.asInstanceOf[IRStoreMD]
          val regionDag = l.filter { case IRStoreRD(_, _, _, _, _) => true; case _ => false }.head.asInstanceOf[IRStoreRD]
          (metaDag, regionDag)
        }
      }

      outputDatasets.map(x => {
        getDagParts(x._2)
      }).toList
    }
    else
      throw new IllegalStateException("The DAG cannot be casted to List[IRVarialbe]")
  }


  /**
    * Set requirement for profile estimation on the descendants of nodeClass
    * @param pred a predicate on the IROperator
    * @return nothing
    */
  def markDown( pred: IROperator => Boolean ): Unit = {
    def _markDown(node:IROperator): Unit = {
      node.requiresOutputProfile = true
      node.getDependencies.foreach(_markDown)
    }
    subDAG(pred).roots.map(_markDown)
  }

  override def create(roots: List[IROperator]): OperatorDAG = new OperatorDAG(roots)
}


class VariableDAG(raw: List[IRVariable]) extends GenericDAG[IRVariable, VariableDAG](raw) {
  override def create(roots: List[IRVariable]): VariableDAG = new VariableDAG(roots)
  def toOperatorDAG: OperatorDAG = new OperatorDAG(this.roots.flatMap(x => List(x.regionDag, x.metaDag)))
}