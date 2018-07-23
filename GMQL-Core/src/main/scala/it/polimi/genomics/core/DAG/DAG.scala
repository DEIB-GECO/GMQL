package it.polimi.genomics.core.DAG

import it.polimi.genomics.core.DataStructures.{IROperator, IRVariable}

class DAG (val raw: List[IROperator]) {

  private val depthWidth: collection.mutable.Map[Int, Int] = collection.mutable.Map[Int, Int]()

  // Auxiliary constructor
  def this(raw: => List[IRVariable]) =  this( raw.flatMap(x=>  List[IROperator](x.regionDag, x.metaDag)) )


  // notice that this method received a tree : non-empty single-root DAG

  /**
    * Recursive private function
    * @param nodeClass Class of IRVariable to be matched
    * @param tree a non-empty DAG with a single root node
    * @return DAG containing all the sub-trees of 'tree' s.t. their root is an instance
    *         of class 'nodeClass'; the result may contain 'tree' itself
    */
  private def _subDAG( nodeClass: Class[_], tree: DAG) : DAG  = {

    val root = tree.raw.head

    var result: DAG =
    if( root.getClass equals nodeClass ) {
      new DAG( List(root) )
    } else {
      new DAG( List[IROperator]() )
    }

    for ( child <- root.getDependencies ) {
      result = result.union( _subDAG(nodeClass, new DAG( List(child) )) )
    }

    result
  }

  // computes the depthWith Map, for each depth the number of nodes at that depth
  private def _depthWidth(root: IROperator, depth: Int): Unit = {

    for ( child <- root.getDependencies ) {
     _depthWidth(child, depth+1)
    }

    depthWidth(depth) =
    if( depthWidth.contains(depth) ) {
      depthWidth(depth)+1
    } else {
      1
    }
  }

  private def _markDown(node:IROperator): Unit = {
    node.requiresOutputProfile = true
    node.getDependencies.foreach(_markDown)
  }


  /**
    * Returns a DAG in which all nodes in the original DAG of class
    * nodeClass become root nodes of the new DAG
    * @param nodeClass
    * @return
    */
  def subDAG( nodeClass : Class[_] ): DAG = {
    raw.map(x => _subDAG(nodeClass,  new DAG( List(x) ) ) ).reduce( (x,y) => x.union(y) )
  }

//  def subDAG(pred: IROperator => Boolean): DAG = {
//    def _subdDAG(dag: DAG, pred:IROperator => Boolean): DAG = {
//
//    }
//    raw.map(x => )
//  }

  def union( other : DAG ) : DAG  = {
    new DAG( raw.union( other.raw ).distinct ) //check it is expectd result
  }

  /**
    * Set requirement for profile estimation on the descendants of nodeClass
    * @param nodeClass
    * @return
    */
  def markDown( nodeClass : Class[_] ): Unit = { subDAG(nodeClass).raw.map(_markDown) }


  def getLeaves(): DAG = ???

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

  def widthAt(depth: Int) = {
    if( depthWidth.isEmpty ) {
      raw.foreach(x => _depthWidth( x , 0 ))
    }
    depthWidth(depth)

  }

  def depth() = {
    if( depthWidth.isEmpty ) {
      raw.foreach(x => _depthWidth( x , 0 ))
    }
    depthWidth.keys.max
  }

  def plot(title:String) = {

    new DAGView(this, title)

  }

}
