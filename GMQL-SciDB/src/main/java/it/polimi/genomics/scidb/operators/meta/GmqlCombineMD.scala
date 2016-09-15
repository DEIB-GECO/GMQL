package it.polimi.genomics.scidb.operators.meta

import it.polimi.genomics.scidb.libraries.gmql4scidb
import it.polimi.genomics.scidb.operators.{GDS, GmqlMetaOperator, GmqlMetaJoinOperator}
import it.polimi.genomics.scidbapi.SciArray
import it.polimi.genomics.scidbapi.expression._
import it.polimi.genomics.scidbapi.script.SciScript

/**
  * This class implements the combineMD it.polimi.genomics.scidb.test.operator that merge two metadata
  * informations for a join command
  *
  * @param metajoin metadata temporary result
  * @param anchor anchor dataset of join operation
  * @param anchorAlias alias fo anchor
  * @param experiment exeriment dataset of join operation
  * @param experimentAlias
  */
class GmqlCombineMD(metajoin:GmqlMetaJoinOperator,
                    anchor:GmqlMetaOperator,
                    anchorAlias:String,
                    experiment:GmqlMetaOperator,
                    experimentAlias:String)
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
    val ANCHOR = anchor.compute(script)
    val EXPERIMENT = experiment.compute(script)

    val METAJOIN = metajoin.compute(script)

    // ------------------------------------------------
    // Combine metadatas ------------------------------

    val ANCHOR_contribution = METAJOIN                                                                                  // keep all the metadata from the anchor
      .cross_join(ANCHOR)((GDS.sid_ANCHOR_D.e,GDS.sid_D.e))                                                       // preparing it
      .redimension(
        List(
          GDS.nid1_D,
          GDS.nid2_D,
          GDS.vid1_D,
          GDS.vid2_D,
          GDS.sid_D.rename(GDS.sid_RESULT_A.name)
        ), GDS.meta_attributes_A
      )
      .cast(GDS.meta_dimensions_D, GDS.meta_attributes_A)

    val ANCHOR_disambiguated = disambiguate(ANCHOR_contribution, anchorAlias)                                           // apply the aliases

    // ------------------------------------------------

    val EXPERIMENT_contribution = METAJOIN                                                                              // keep all the metadata from the experiment
      .cross_join(EXPERIMENT)((GDS.sid_EXPERIMENT_D.e,GDS.sid_D.e))                                               // preparing it
      .redimension(
        List(
          GDS.nid1_D,
          GDS.nid2_D,
          GDS.vid1_D,
          GDS.vid2_D,
          GDS.sid_D.rename(GDS.sid_RESULT_A.name)
        ), GDS.meta_attributes_A
      )
      .cast(GDS.meta_dimensions_D, GDS.meta_attributes_A)

    val EXPERIMENT_disambiguated = disambiguate(EXPERIMENT_contribution, experimentAlias)                               // apply the aliases

    // ------------------------------------------------

    val MD_combined = ANCHOR_disambiguated merge EXPERIMENT_disambiguated                                               // combine the two partial result to obtain
                                                                                                                        // the result metadata
    // ------------------------------------------------
    // ------------------------------------------------

    MD_combined
  }


  /**
    * This method applies a disambiguation on an array containing
    * metadata, using the specified alias
    *
    * @param array metadata array
    * @param name alias
    * @return disambiguated array
    */
  def disambiguate(array:SciArray, name:String) : SciArray =
  {
    val name_new_A = GDS.name_A.rename("name_new")

    // ------------------------------------------------

    val disambiguated = array
      .apply((name_new_A.e, OP(V(name+"."), "+", GDS.name_A.e)))                                                  // define the new name for the metadata
      .apply(
        (A(GDS.nid1_D.name+"_new"), LIB(gmql4scidb.hash, name_new_A.e)),                                                  // define the new hashes for the name
        (A(GDS.nid2_D.name+"_new"), LIB(gmql4scidb.hash, OP(name_new_A.e,"+",C_STRING(V("0")))))
      )
      .cast(                                                                                                            // cast the names in order to use the
        List(                                                                                                           // new fields
          GDS.nid1_D.rename("nid_1_DEL"),
          GDS.nid2_D.rename("nid_2_DEL"),
          GDS.vid1_D, GDS.vid2_D, GDS.sid_D
        ), List(
          GDS.name_A.rename("name_DEL"),
          GDS.value_A,
          GDS.name_A,
          GDS.nid1_D.toAttribute(),
          GDS.nid2_D.toAttribute()
        )
      )
      .redimension(GDS.meta_dimensions_D, GDS.meta_attributes_A)                                                        // redimension the result as a metadata array

    // ------------------------------------------------

    disambiguated
  }

}
