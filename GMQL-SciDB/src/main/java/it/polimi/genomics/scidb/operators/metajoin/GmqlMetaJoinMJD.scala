package it.polimi.genomics.scidb.operators.metajoin

import it.polimi.genomics.core.DataStructures.MetaJoinCondition.MetaJoinCondition
import it.polimi.genomics.scidb.libraries.gmql4scidb
import it.polimi.genomics.scidb.operators.{GDS, GmqlMetaJoinOperator, GmqlMetaOperator}
import it.polimi.genomics.scidbapi.SciArray
import it.polimi.genomics.scidbapi.expression._
import it.polimi.genomics.scidbapi.schema.DataType._
import it.polimi.genomics.scidbapi.schema.Attribute
import it.polimi.genomics.scidbapi.script.SciScript

/**
  * This it.polimi.genomics.scidb.test.operator perform the metajoin preparation on the metadata
  * extracting the list of the sample id pairs that should be computed
  * in the final join
  *
  * @param anchor anchor dataset of the join
  * @param experiment experiment dataset of the join
  * @param conditions metajoin conditions
  */
class GmqlMetaJoinMJD(anchor:GmqlMetaOperator,
                      experiment:GmqlMetaOperator,
                      conditions:MetaJoinCondition)
  extends GmqlMetaJoinOperator
{
  this._stored = true

  /**
    * Apply the it.polimi.genomics.scidb.test.operator producing the result, it requires the
    * child nodes results if necessary
    *
    * @param script Context script
    */
  override def apply(script: SciScript): SciArray =
  {
    val ANCHOR = anchor.compute(script)
    val EXPERIMENT = experiment.compute(script)

    val sid_ANCHOR = GDS.sid_ANCHOR_D.toAttribute()
    val sid_EXPERIMENT = GDS.sid_EXPERIMENT_D.toAttribute()

    var MJD : SciArray = null

    if( conditions.attributes.isEmpty ){

      // ------------------------------------------------
      // Complete cross join ----------------------------

      val aSID = ANCHOR                                                                                                 // get all the sid from anchor
        .redimension(List(GDS.sid_D), GDS.meta_attributes_A)                                                            // redimensioning it
        .apply((sid_ANCHOR.e, GDS.sid_D.e))
        .project(sid_ANCHOR.e)

      val eSID = EXPERIMENT                                                                                             // get all the sid from experiment
        .redimension(List(GDS.sid_D), GDS.meta_attributes_A)                                                            // redimensioning it
        .apply((sid_EXPERIMENT.e, GDS.sid_D.e))
        .project(sid_EXPERIMENT.e)

      MJD = aSID                                                                                                        // join the two list producing the
        .cross_join(eSID)()                                                                                             // complete cross join
        .apply((GDS.sid_RESULT_A.e, OP(OP(sid_ANCHOR.e, "*", V(10000)), "+", sid_EXPERIMENT.e)))               // TODO: define final function
        .redimension(GDS.metajoin_dimensions_D, GDS.metajoin_attributes_A)

      // ------------------------------------------------
      // ------------------------------------------------

    }else{

      // ------------------------------------------------
      // Group by condition -----------------------------

      val attrSID = conditions.attributes
        .map(attribute => {                                                                                             // for each groupby condition

          val aPLANE = ANCHOR                                                                                           // extract the metadata plane
            .filter(                                                                                                    // for a certain attribute name
              AND(                                                                                                      // from the anchor
                OP(GDS.nid1_D.e, "=", LIB(gmql4scidb.hash, V(attribute))),
                OP(GDS.nid2_D.e, "=", LIB(gmql4scidb.hash, OP(V(attribute),"+",C_STRING(V("0")))))
              )
            )
            .apply((sid_ANCHOR.e, GDS.sid_D.e))
            .redimension(List(GDS.vid1_D, GDS.vid2_D, GDS.sid_D), List(sid_ANCHOR))

          val ePLANE = EXPERIMENT                                                                                       // extract the metadata plane
            .filter(                                                                                                    // for a certain attribute name
              AND(                                                                                                      // from the experiment
                OP(GDS.nid1_D.e, "=", LIB(gmql4scidb.hash, V(attribute))),
                OP(GDS.nid2_D.e, "=", LIB(gmql4scidb.hash, OP(V(attribute),"+",C_STRING(V("0")))))
              )
            )
            .apply((sid_EXPERIMENT.e, GDS.sid_D.e))
            .redimension(List(GDS.vid1_D, GDS.vid2_D, GDS.sid_D), List(sid_EXPERIMENT))

          aPLANE                                                                                                        // join the two sid list obtaining
            .cross_join(ePLANE, "ANCHOR", "EXPERIMENT")(                                                                // valid pairs for a certain
              (GDS.vid1_D.alias("ANCHOR").e, GDS.vid1_D.alias("EXPERIMENT").e),                                   // joinby condition
              (GDS.vid2_D.alias("ANCHOR").e, GDS.vid2_D.alias("EXPERIMENT").e)
            )
            .apply((GDS.sid_RESULT_A.e, OP(OP(sid_ANCHOR.e, "*", V(10000)), "+", sid_EXPERIMENT.e)))           // TODO: define final function
            .redimension(GDS.metajoin_dimensions_D, GDS.metajoin_attributes_A)
        })

      MJD = attrSID.reduce((A1, A2) => A1
        .cross_join
          (A2.cast(GDS.metajoin_dimensions_D, List(GDS.sid_RESULT_A.label("RESULT_TMP"))), "A1", "A2")(                 // reduce the result considering
          (GDS.sid_ANCHOR_D.alias("A1").e, GDS.sid_ANCHOR_D.alias("A2").e),                                       // just the pairs intersection
          (GDS.sid_EXPERIMENT_D.alias("A1").e, GDS.sid_EXPERIMENT_D.alias("A2").e) )
        .project(GDS.sid_RESULT_A.e)
      )

      // ------------------------------------------------
      // ------------------------------------------------

    }

    MJD
  }

}
