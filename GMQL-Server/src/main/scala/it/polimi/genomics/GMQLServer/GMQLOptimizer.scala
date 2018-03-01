package it.polimi.genomics.GMQLServer
import it.polimi.genomics.core.DataStructures.ExecutionParameters.BinningParameter
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

  var binS: Option[BinningParameter]

  /**
    * Default optimization of the DAG. Nothing is done
    * @param dag
    * @return the same dag
    */
  def optimize(dag: List[IRVariable]) : List[IRVariable] = dag
  def setBinningParameter(binS: BinningParameter): Unit = {
    this.binS = Some(binS)
  }
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
  var binS: Option[BinningParameter] = None
  override def optimize(dag: List[IRVariable]): List[IRVariable] = dag
}



