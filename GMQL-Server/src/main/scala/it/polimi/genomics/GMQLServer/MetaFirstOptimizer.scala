package it.polimi.genomics.GMQLServer
import it.polimi.genomics.core.DataStructures._

/**
  * Created by Luca Nanni on 16/11/17.
  * Email: luca.nanni@mail.polimi.it
  */

/**
  * Implements the Meta-First optimization as described in the paper
  * "Metadata Management for Scientific Databases" by Pietro Pinoli,
  * Stefano Ceri, Davide Martinenghi and Luca Nanni
  */
class MetaFirstOptimizer(decoratedOptimizer: GMQLOptimizer) extends GMQLOptimizerDecorator(decoratedOptimizer) {

  /**
    * Performs the optimization
    * @param dag
    * @return optimized dag
    */
  override def optimize(dag: List[IRVariable]): List[IRVariable] = {
    // The optimization is applied to each variable singularly
    super.optimize(dag).map(optimizeMetaFirst)
  }


  /**
    * Applies the optimization to each IRVariable inside the DAG
    * @param dag
    * @return the new IRVariable
    */
  private def optimizeMetaFirst(dag: IRVariable): IRVariable = {
    if(!isMetaSeparable(dag)) {
      // if the query is not meta-separable, just return the old dag
      println("NOT META-SEPARABLE")
      dag
    }
    else {
      println("META-SEPARABLE")
      dag
      //TODO: finish
    }
  }

  /**
    * A query is meta-separable if there are no Metadata operations that take as input a Region operation node.
    *
    * Currently the following MetaOperator nodes make a query NOT meta-separable
    *
    * - IRPurgeMD
    * - IRGroupMD
    * - IRAggregateRD
    *
    * @param dag: an IRVariable representing the dag of the query
    * @return a boolean saying True if the query is meta-separable and False if not
    */
  private def isMetaSeparable(dag: IRVariable): Boolean = {
    val metaDAG = dag.metaDag

    def checkSingle(operator: IROperator): Boolean = {
      operator match {
        case IRPurgeMD(_,_) | IRGroupMD(_,_,_,_,_) | IRAggregateRD(_,_) => false
        case _ => true
      }
    }

    def check(operator : IROperator) : Boolean = {
      val metaOperations = operator.getOperatorList
      if(metaOperations.isEmpty)
        checkSingle(operator)
      else
        checkSingle(operator) && metaOperations.forall(check)
    }
    check(metaDAG)
  }

}
