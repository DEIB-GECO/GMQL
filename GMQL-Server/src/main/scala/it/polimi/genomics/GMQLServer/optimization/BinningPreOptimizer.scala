package it.polimi.genomics.GMQLServer.optimization

import it.polimi.genomics.core.DAG.DAG
import it.polimi.genomics.core.DataStructures.ExecutionParameters.BinningParameter
import it.polimi.genomics.core.DataStructures.{IRGenometricJoin, IRGenometricMap, IRVariable}

class BinningPreOptimizer (decoratedOptimizer: GMQLOptimizer) extends GMQLOptimizerDecorator(decoratedOptimizer){

  override var binS: Option[BinningParameter] = None

  override def optimize(dag: List[IRVariable]): List[IRVariable] = {
    val dagHelper = new DAG(dag)
    dagHelper.markDown(classOf[IRGenometricJoin]).markDown(classOf[IRGenometricMap])
    dag
  }


}
