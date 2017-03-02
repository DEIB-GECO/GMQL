package it.polimi.genomics.scidb.operators.meta

import it.polimi.genomics.scidb.operators.{GDS, GmqlMetaGroupOperator, GmqlMetaOperator}
import it.polimi.genomics.scidbapi.SciArray
import it.polimi.genomics.scidbapi.expression.V
import it.polimi.genomics.scidbapi.script.SciScript

/**
  * This it.polimi.genomics.scidb.test.operator implements the merge of the
  * metadata starting from a dataset and a grouping rule
  *
  * @param source source dataset
  * @param groups group condition
  */
class GmqlMergeMD(source : GmqlMetaOperator,
                  groups : Option[GmqlMetaGroupOperator])
  extends GmqlMetaOperator
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
    // grouping ---------------------------------------

    val GROUPED = groups match
    {
      case None => SOURCE
        .apply((GDS.gid_A.e, V(1)))
        .redimension(GDS.meta_dimensions_D ::: List(GDS.gid_D), GDS.meta_attributes_A)

      case Some(groups) =>
        SOURCE
          .cross_join(groups.compute(script), "SOURCE", "GROUPS")((GDS.sid_D.alias("SOURCE").e, GDS.sid_D.alias("GROUPS").e))
          .redimension(GDS.meta_dimensions_D ::: List(GDS.gid_D), GDS.meta_attributes_A)
    }

    // ------------------------------------------------
    // merge ------------------------------------------

    val RESULT = GROUPED
      .redimension(List(GDS.nid1_D, GDS.nid2_D, GDS.vid1_D, GDS.vid2_D, GDS.gid_D), GDS.meta_attributes_A)
      .cast(GDS.meta_dimensions_D, GDS.meta_attributes_A)

    // ------------------------------------------------

    RESULT
  }

}
