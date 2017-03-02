package it.polimi.genomics.scidb.operators.region

import it.polimi.genomics.core.DataStructures.IRDataSet
import it.polimi.genomics.scidb.operators.{GDS, GmqlRegionOperator}
import it.polimi.genomics.scidb.utility.SchemaUtils
import it.polimi.genomics.scidbapi.SciArray
import it.polimi.genomics.scidbapi.script.SciScript

/**
  * Created by Cattani Simone on 14/05/16.
  * Email: simone.cattani@mail.polimi.it
  *
  */
class GmqlReadLightRD(dataset : IRDataSet)
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
  { import scala.collection.JavaConverters._
    new SciArray(
      List(GDS.sid_D, GDS.chr_D, GDS.enumeration_D),
      dataset.schema.asScala.toList.map(item => SchemaUtils.toAttribute(item._1, item._2)),
      dataset.position +"_RD"
    )
  }
}