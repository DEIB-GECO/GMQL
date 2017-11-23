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

/**
  * The GMQL Optimizers are implemented with the Decorator Pattern.
  * In this way it is enabled the stacking of various optimizers.
  *
  * @param decoratedOptimizer
  */
abstract class GMQLOptimizerDecorator(decoratedOptimizer: GMQLOptimizer) extends GMQLOptimizer{
  override def optimize(dag: List[IRVariable]): List[IRVariable] = decoratedOptimizer.optimize(dag)
}

/**
  * The default optimizer. It does nothing to the DAG.
  */
class DefaultOptimizer extends GMQLOptimizer {
  override def optimize(dag: List[IRVariable]): List[IRVariable] = dag
}

//class DAGTree(root: DAGNode) {
//  def getLeaves = root.getLeaves
//
//  def getNodes = root.getNodes
//}
//
//class DAGNode(val value: IROperator, val parents: List[DAGNode]){
//  def getLeaves: List[DAGNode] = {
//    if(this.parents.nonEmpty){
//      this.parents.flatMap(x => x.getLeaves)
//    }
//    else {
//      List(this)
//    }
//  }
//
//  def getNodes: List[DAGNode] = {
//    List(this) :: this.parents.flatMap(x => x.getNodes))
//    //TODO: finish
//  }
//}


