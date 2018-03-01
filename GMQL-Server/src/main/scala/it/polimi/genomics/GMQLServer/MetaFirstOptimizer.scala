package it.polimi.genomics.GMQLServer
import it.polimi.genomics.core.DataStructures.ExecutionParameters.BinningParameter
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
class MetaFirstOptimizer(decoratedOptimizer: GMQLOptimizer)
  extends GMQLOptimizerDecorator(decoratedOptimizer) {
  var binS: Option[BinningParameter] = None

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
    val newDag = dag.copy()(this.binS.get)
    if(!isMetaSeparable(newDag)) {
      // if the query is not meta-separable, just return the old dag
      println("NOT META-SEPARABLE")
      dag
    }
    else {
      println("META-SEPARABLE")
      val metaDAGBeforeStore: MetaOperator = {
        newDag.metaDag match {
          case x: IRStoreMD => x.father
          case _ => throw new IllegalStateException("MetaDAG is malformed. IRSTOREMD not last operator")
        }
      }
      val newRegionDag = applyMetaPatch(newDag.regionDag, metaDAGBeforeStore)
      newDag.copy(regionDag = newRegionDag)(this.binS.get)
    }
  }

  private def applyMetaPatch(regionDag: RegionOperator, metaPatch: MetaOperator): RegionOperator = {
    regionDag
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
    checkMetaToRegion(dag.metaDag) && checkMetaToRegion(dag.regionDag)
  }

  private def checkMetaToRegion(op: IROperator): Boolean = {
    // The node is META and has a REGION children --> not meta-separable
    if(op.isMetaOperator && op.getRegionChildren.nonEmpty)
      false
    // there are some children --> recursion
    else if(op.getChildren.nonEmpty)
      op.getChildren.map(checkMetaToRegion).forall(identity)
    else
    // there are no children --> True
      true
  }

}
