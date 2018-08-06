package it.polimi.genomics.core.DAG

import it.polimi.genomics.core.DataStructures.ExecutionParameters.BinningParameter
import it.polimi.genomics.core.DataStructures._
import it.polimi.genomics.core.ParsingType.PARSING_TYPE

class DAG (val raw: List[IROperator]) {
  private val depthWidth: collection.mutable.Map[Int, Int] = collection.mutable.Map[Int, Int]()
  // Auxiliary constructor
  def this(raw: => List[IRVariable]) =  this( raw.flatMap(x=>  List[IROperator](x.regionDag, x.metaDag)) )


  // For a DAG to be Variable-consistent it must not have any root node which is different from
  // IRStoreMD and IRStoreRD
  private def checkVariableConsistency: Boolean = !this.raw.exists {
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
      val outputDatasets: Map[IRDataSet, List[IROperator]] = this.raw.map(getMap)
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


  /** Returns a DAG in which all the nodes of the original DAG which
    * satisfy the given predicate become root nodes of the new DAG
    *
    * @param pred: a predicate on the IROperator
    * @return a new DAG
    */
  def subDAG(pred: IROperator => Boolean): DAG = {
    def _subDAG(ops: List[IROperator], pred:IROperator => Boolean): List[IROperator] = {
      if (ops.isEmpty) List[IROperator]() else ops.filter(pred) ++ _subDAG(ops.flatMap(x => x.getDependencies), pred)
    }
    _subDAG(this.raw, pred).map(x => new DAG(List(x))).reduce( (x,y) => x.union(y) )
  }


  def union( other : DAG ) : DAG  = {
    new DAG( raw.union( other.raw ).distinct ) //check it is expectd result
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
    subDAG(pred).raw.map(_markDown)
  }

  // computes the depthWith Map, for each depth the number of nodes at that depth
  private def _depthWidth(root: IROperator, depth: Int): Unit = {

    for ( child <- root.getDependencies ) {
      _depthWidth(child, depth+1)
    }
    depthWidth(depth) = if( depthWidth.contains(depth) ) depthWidth(depth)+1 else 1
  }

  /**
    *  mMximum number of elements having the same depth
    * @return
    */
  def maxWidth():Int = {
    if( depthWidth.isEmpty ) {
      raw.foreach(x => _depthWidth( x , 0 ))
    }
    depthWidth.values.max
  //  val depth = depthWidth.filter( x => x._2==max )
  }

  def widthAt(depth: Int): Int = {
    if( depthWidth.isEmpty ) {
      raw.foreach(x => _depthWidth( x , 0 ))
    }
    depthWidth(depth)

  }

  def depth(): Int = {
    if( depthWidth.isEmpty ) {
      raw.foreach(x => _depthWidth( x , 0 ))
    }
    depthWidth.keys.max
  }

  def plot(title:String): DAGView = {

    new DAGView(this, title)

  }

  //  /**
  //    * Returns a DAG in which all nodes in the original DAG of class
  //    * nodeClass become root nodes of the new DAG
  //    * @param nodeClass
  //    * @return
  //    */
  //  def subDAG( nodeClass : Class[_] ): DAG = {
  //
  //    /**
  //      * Recursive private function
  //      * @param nodeClass Class of IRVariable to be matched
  //      * @param tree a non-empty DAG with a single root node
  //      * @return DAG containing all the sub-trees of 'tree' s.t. their root is an instance
  //      *         of class 'nodeClass'; the result may contain 'tree' itself
  //      */
  //    def _subDAG( nodeClass: Class[_], tree: DAG) : DAG  = {
  //      val root = tree.raw.head
  //      var result: DAG =
  //        if( root.getClass equals nodeClass ) {
  //          new DAG( List(root) )
  //        } else {
  //          new DAG( List[IROperator]() )
  //        }
  //      for ( child <- root.getDependencies ) {
  //        result = result.union( _subDAG(nodeClass, new DAG( List(child) )) )
  //      }
  //      result
  //    }
  //
  //    raw.map(x => _subDAG(nodeClass,  new DAG( List(x) ) ) ).reduce( (x,y) => x.union(y) )
  //  }

}
