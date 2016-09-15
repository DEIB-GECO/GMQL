package it.polimi.genomics.scidb.operators.region

import it.polimi.genomics.core.DataStructures.IRDataSet
import it.polimi.genomics.scidb.operators.{GDS, GmqlRegionOperator}
import it.polimi.genomics.scidb.utility.SchemaUtils
import it.polimi.genomics.scidbapi.SciArray
import it.polimi.genomics.scidbapi.script.SciScript

/**
  * Implements the read operation for the region data
  *
  * @param dataset input dataset
  */
class GmqlReadRD(dataset : IRDataSet)
  extends GmqlRegionOperator
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
    import scala.collection.JavaConverters._
    new SciArray(
      GDS.regions_dimensions_D,
      dataset.schema.asScala.map(item => SchemaUtils.toAttribute(item._1, item._2)).toList,
      dataset.position +"_RD"
    )
  }
}
