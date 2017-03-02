package it.polimi.genomics.scidb.operators.region

import it.polimi.genomics.scidb.operators.{GDS, GmqlRegionOperator, GmqlMetaGroupOperator}
import it.polimi.genomics.scidbapi.SciArray
import it.polimi.genomics.scidbapi.expression.V
import it.polimi.genomics.scidbapi.script.SciScript


/**
  * This it.polimi.genomics.scidb.test.operator implements the merge of the
  * regions starting from a dataset and a grouping rule
  *
  * @param source source dataset
  * @param groups group condition
  */
class GmqlMergeRD(source : GmqlRegionOperator,
                  groups : Option[GmqlMetaGroupOperator])
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
    // GROUPING ---------------------------------------

    val GROUPED = groups match
    {
      case None => SOURCE
        .apply((GDS.gid_A.e, V(1)))
        .redimension(GDS.regions_dimensions_D ::: List(GDS.gid_D), SOURCE.getAttributes())

      case Some(groups) => {
        SOURCE
          .cross_join(groups.compute(script), "SOURCE", "GROUPS")((GDS.sid_D.alias("SOURCE").e, GDS.sid_D.alias("GROUPS").e))
          .redimension(GDS.regions_dimensions_D ::: List(GDS.gid_D), SOURCE.getAttributes())  }
    }

    // ------------------------------------------------
    // MERGE ------------------------------------------

    val RESULT = GROUPED
      .unpack(GDS.enumeration_D.label("new").e, GDS.enumeration_D.chunk_length)
      .redimension(List(GDS.gid_D, GDS.chr_D, GDS.left_D, GDS.right_D, GDS.strand_D, GDS.enumeration_D.label("new")), SOURCE.getAttributes())
      .cast(GDS.regions_dimensions_D, SOURCE.getAttributes())

    // ------------------------------------------------

    RESULT
  }

}
