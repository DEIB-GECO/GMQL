package it.polimi.genomics.GMQLServer
import it.polimi.genomics.core.DataStructures.IRVariable

/**
  * Created by Luca Nanni on 16/11/17.
  * Email: luca.nanni@mail.polimi.it
  */

/**
  * Implements the Meta-First optimization as described in the paper
  * "Metadata Management for Scientific Databases" by Pietro Pinoli,
  * Stefano Ceri, Davide Martinenghi and Luca Nanni
  */
class MetaFirstOptimizer extends GMQLOptimizer {

  /**
    * Performs the optimization
    * @param dag
    * @return optimized dag
    */
  override def optimize(dag: List[IRVariable]): List[IRVariable] = {
    // The optimization is applied to each variable singularly
    dag.map(optimizeMetaFirst)
  }


  private def optimizeMetaFirst(dag: IRVariable): IRVariable = {
    if(!isMetaSeparable(dag))
      dag
    else {
      dag
      //TODO: finish
    }
  }

  private def isMetaSeparable(dag: IRVariable): Boolean = {
    false
  }

}
