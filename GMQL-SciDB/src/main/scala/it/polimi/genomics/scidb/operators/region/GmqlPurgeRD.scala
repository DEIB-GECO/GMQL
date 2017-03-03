package it.polimi.genomics.scidb.operators.region

import it.polimi.genomics.scidb.operators.{GDS, GmqlMetaOperator, GmqlRegionOperator}
import it.polimi.genomics.scidbapi.SciArray
import it.polimi.genomics.scidbapi.script.SciScript


class GmqlPurgeRD(source : GmqlRegionOperator,
                  metadata : GmqlMetaOperator)
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
    val METADATA = metadata.compute(script)

    // ------------------------------------------------
    // Purge filter -----------------------------------

    val FILTER = METADATA
      .redimension(List(GDS.sid_D), GDS.meta_attributes_A)

    val RESULT = SOURCE
      .cross_join(FILTER, "SOURCE", "FILTER")((GDS.sid_D.alias("SOURCE").e, GDS.sid_D.alias("FILTER").e))
      .project(SOURCE.getAttributes().map(_.e) : _*)

    // ------------------------------------------------

    RESULT
  }

}
