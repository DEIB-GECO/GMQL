package it.polimi.genomics.scidb.operators.region

import it.polimi.genomics.core.DataStructures.IRDataSet
import it.polimi.genomics.scidb.operators.{GmqlRegionOperator}
import it.polimi.genomics.scidbapi.SciArray
import it.polimi.genomics.scidbapi.script.SciScript

class GmqlStoreRD(dataset : IRDataSet,
                  child : GmqlRegionOperator)
  extends GmqlRegionOperator
{
  // ------------------------------------------------------------

  this._stored = false
  this._storing_temp = false
  this._storing_name = None

  child._stored = true
  child._storing_temp = false
  child._storing_name = Some(dataset.position +"_RD")

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
