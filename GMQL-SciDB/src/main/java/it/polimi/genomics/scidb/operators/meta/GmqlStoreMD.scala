package it.polimi.genomics.scidb.operators.meta

import it.polimi.genomics.core.DataStructures.IRDataSet
import it.polimi.genomics.scidb.operators.GmqlMetaOperator
import it.polimi.genomics.scidbapi.SciArray
import it.polimi.genomics.scidbapi.script.SciScript

/**
  * Implements the store it.polimi.genomics.scidb.test.operator for the metadata
 *
  * @param dataset final dataset
  * @param child child it.polimi.genomics.scidb.test.operator
  */
final class GmqlStoreMD(dataset:IRDataSet, child:GmqlMetaOperator)
  extends GmqlMetaOperator
{
  // ------------------------------------------------------------

  this._stored = true
  this._storing_temp = false
  this._storing_name = Some(dataset.position +"_MD")

  this.use                      // prevent the result remove

  // ------------------------------------------------------------

  /**
    * Apply the it.polimi.genomics.scidb.test.operator producing the result, it requires the
    * child nodes results if necessary
    *
    * @param script Context script
    */
  override def apply(script: SciScript): SciArray =
  {
    child.compute(script)       // simply return the current array,
                                // that will be materialized
  }
}
