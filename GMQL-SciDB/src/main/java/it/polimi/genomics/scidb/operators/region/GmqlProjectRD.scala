package it.polimi.genomics.scidb.operators.region

import it.polimi.genomics.scidb.libraries.gmql4scidb
import it.polimi.genomics.scidb.operators.{GDS, GmqlRegionOperator}
import it.polimi.genomics.scidb.utility.DebugUtils
import it.polimi.genomics.scidbapi.SciArray
import it.polimi.genomics.scidbapi.expression._
import it.polimi.genomics.scidbapi.script.SciScript

/**
  * Created by Cattani Simone on 14/04/16.
  * Email: simone.cattani@mail.polimi.it
  *
  */
class GmqlProjectRD(source:GmqlRegionOperator, features:List[Int])
  extends GmqlRegionOperator
{
  /**
    * Apply the it.polimi.genomics.scidb.test.operator producing the result, it requires the
    * child nodes results if necessary
    *
    * @param script Context script
    */
  override def apply(script: SciScript): SciArray =
  {
    val SOURCE = source.compute(script)

    // ------------------------------------------------
    // ------------------------------------------------

    val PROJECTION = if(features.isEmpty) SOURCE else SOURCE
      .redimension(
        GDS.regions_dimensions_D,
        features.map(f => SOURCE.getAttributes()(f))
      )

    // ------------------------------------------------
    // ------------------------------------------------

    DebugUtils.exec(PROJECTION, script)

    PROJECTION
  }

}
