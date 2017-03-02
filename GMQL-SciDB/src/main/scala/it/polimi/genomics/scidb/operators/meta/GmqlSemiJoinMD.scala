package it.polimi.genomics.scidb.operators.meta

import it.polimi.genomics.core.DataStructures.MetaJoinCondition.MetaJoinCondition
import it.polimi.genomics.scidb.libraries.gmql4scidb
import it.polimi.genomics.scidb.operators.{GDS, GmqlMetaOperator}
import it.polimi.genomics.scidbapi.SciArray
import it.polimi.genomics.scidbapi.expression._
import it.polimi.genomics.scidbapi.script.SciScript


/**
  * This it.polimi.genomics.scidb.test.operator implements the semi join selection
  * on metadata
  *
  * @param source source dataset
  * @param external external dataset
  * @param conditions semi join conditions
  */
class GmqlSemiJoinMD(source : GmqlMetaOperator,
                     external : GmqlMetaOperator,
                     conditions : MetaJoinCondition)
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
    val EXTERNAL = external.compute(script)

    // ------------------------------------------------
    // Samples ids filter definition ------------------

    val SIDS = conditions.attributes.map( attr =>

      SOURCE
        .filter( AND(OP(GDS.nid1_D.e, "=", LIB(gmql4scidb.hash, V(attr))), OP(GDS.nid2_D.e, "=", LIB(gmql4scidb.hash, OP(V(attr),"+",C_STRING(V("0")))))) )
        .redimension(List(GDS.vid1_D, GDS.vid2_D, GDS.sid_D), GDS.meta_attributes_A)
        .cross_join(

          EXTERNAL
            .filter( AND(OP(GDS.nid1_D.e, "=", LIB(gmql4scidb.hash, V(attr))), OP(GDS.nid2_D.e, "=", LIB(gmql4scidb.hash, OP(V(attr),"+",C_STRING(V("0")))))) )
            .redimension(List(GDS.vid1_D, GDS.vid2_D, GDS.sid_D), GDS.meta_attributes_A),

          "SOURCE", "EXTERNAL"
        )(
          (GDS.vid1_D.alias("SOURCE").e, GDS.vid1_D.alias("EXTERNAL").e),
          (GDS.vid2_D.alias("SOURCE").e, GDS.vid2_D.alias("EXTERNAL").e),
          (GDS.sid_D.alias("SOURCE").e, GDS.sid_D.alias("EXTERNAL").e)
        )
        .redimension(List(GDS.sid_D), GDS.meta_attributes_A)
        .apply((GDS.null1, V(1))).project(GDS.null1)

    ).reduce((A1, A2) => A1.cross_join(A2, "F", "S")((GDS.sid_D.alias("F").e, GDS.sid_D.alias("S").e)))

    // ------------------------------------------------
    // Samples ids filter application -----------------

    val SELECTED = SOURCE
      .cross_join(SIDS, "SOURCE", "FILTER")((GDS.sid_D.alias("SOURCE").e, GDS.sid_D.alias("FILTER").e))
      .project(GDS.meta_attributes_A.map(_.e) : _*)

    // ------------------------------------------------

    SELECTED
  }

}
