package it.polimi.genomics.GMQLServer

import it.polimi.genomics.core.DataStructures.{IROperator, IRVariable}

/**
  * Created by Luca Nanni on 16/11/17.
  * Email: luca.nanni@mail.polimi.it
  */

/**
  * A GMQLOptimizer is an object that receives a DAG and outputs a
  * "rewritten" DAG which is optimized in some way
  * */
trait GMQLOptimizer {

  /**
    * Default optimization of the DAG. Nothing is done
    * @param dag
    * @return the same dag
    */
  def optimize(dag: List[IRVariable]) : List[IRVariable] = dag

}

class DAGTree(root: DAGNode) {
  def getLeaves = root.getLeaves

  def getNodes = root.getNodes
}

class DAGNode(val value: IROperator, val parents: List[DAGNode]){
  def getLeaves: List[DAGNode] = {
    if(this.parents.nonEmpty){
      this.parents.flatMap(x => x.getLeaves)
    }
    else {
      List(this)
    }
  }

  def getNodes: List[DAGNode] = {
    List(this) :: this.parents.flatMap(x => x.getNodes))
    //TODO: finish
  }
}


