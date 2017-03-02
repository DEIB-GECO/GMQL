package it.polimi.genomics.scidb.operators.meta

import it.polimi.genomics.core.DataStructures.IRDataSet
import it.polimi.genomics.scidb.operators.{GDS, GmqlMetaOperator}
import it.polimi.genomics.scidbapi.SciArray
import it.polimi.genomics.scidbapi.script.SciScript


/**
  * Implements the read operation for the metadata
  *
  * @param dataset input dataset
  */
class GmqlReadMD(dataset:IRDataSet)
  extends GmqlMetaOperator
{
  this._stored = false

  /**
    * Apply the it.polimi.genomics.scidb.test.operator producing the result, it requires the
    * child nodes results if necessary
    *
    * @param script Context script
    */
  override def apply(script: SciScript): SciArray =
  {
    new SciArray(
      GDS.meta_dimensions_D,
      GDS.meta_attributes_A,
      dataset.position +"_MD"
    )
  }
}
