package it.polimi.genomics.scidb.operators.region

import it.polimi.genomics.core.DataStructures.RegionCondition.RegionCondition
import it.polimi.genomics.scidb.GmqlSciConfig
import it.polimi.genomics.scidb.evaluators.RegionConditionsEvaluator
import it.polimi.genomics.scidb.libraries.gmql4scidb
import it.polimi.genomics.scidb.operators.{GDS, GmqlMetaOperator, GmqlRegionOperator}
import it.polimi.genomics.scidbapi.{SciStoredArray, SciArray}
import it.polimi.genomics.scidbapi.expression._
import it.polimi.genomics.scidbapi.script.SciScript

/**
  * This it.polimi.genomics.scidb.test.operator implements the region selection according
  * to the required conditions
  *
  * @param source original source
  * @param metafilter filter applied on metadatas
  * @param conditions conditions to be applied on regions
  */
class GmqlSelectRD(source : GmqlRegionOperator,
                   metafilter : Option[GmqlMetaOperator],
                   conditions : Option[RegionCondition])
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
    val METAINPUT = metafilter match {
      case None => None
      case Some(mf) => Some(mf.compute(script))
    }

    // ------------------------------------------------
    // ------------------------------------------------

    if(GmqlSciConfig.select_bypass)
      return SOURCE

    // ------------------------------------------------
    // Meta filter ------------------------------------

    val METAFILTERED = if( /*true ||*/ METAINPUT.isEmpty ) SOURCE else
    {
      val METAFILTER = METAINPUT.get
        .redimension(List(GDS.sid_D), GDS.meta_attributes_A)
        .apply((GDS.null1, V(1))).project(GDS.null1)

      SOURCE
        .cross_join(METAFILTER, "SOURCE", "FILTER")((GDS.sid_D.alias("SOURCE").e, GDS.sid_D.alias("FILTER").e))
        .project(SOURCE.getAttributes().map(_.e) : _*)
    }

    // ------------------------------------------------
    // Region filter ----------------------------------

    val REGIONFILTERED = if( conditions.isEmpty ) METAFILTERED else
    {
      val RD_CHR = new SciStoredArray( List(GDS.chri_D), List(GDS.CHR_A), "RD_CHR", "RD_CHR" )

      val EXTENDED = METAFILTERED
        .cross_join(RD_CHR.reference(), "REGIONS", "CHR")((GDS.chr_D.alias("REGIONS").e, GDS.chri_D.alias("CHR").e))

      // ----------------------------------------------
      // ----------------------------------------------

      val evaluation = RegionConditionsEvaluator(EXTENDED, conditions.get)

      val EXTENDED2 = if( evaluation._2.isEmpty || METAINPUT.isEmpty ) EXTENDED else
      {
        val METAEXTRACTED = evaluation._2.map(item =>
          METAINPUT.get
            .filter( AND(OP(GDS.nid1_D.e, "=", LIB(gmql4scidb.hash, V(item._1))), OP(GDS.nid2_D.e, "=", LIB(gmql4scidb.hash, OP(V(item._1),"+",C_STRING(V("0")))))) )
            .redimension(List(GDS.sid_D), GDS.meta_attributes_A)
            .project(GDS.value_A.e)
            .cast(List(GDS.sid_D), List(GDS.value_A.rename(item._1+"_"+item._2)))
        ).reduce((A1,A2) =>
          A1.cross_join(A2, "F", "S")((GDS.sid_D.alias("F").e, GDS.sid_D.alias("S").e))
        )

        EXTENDED.cross_join(METAEXTRACTED, "REGION", "META")((GDS.sid_D.alias("REGION").e, GDS.sid_D.alias("META").e))
      }

      // ----------------------------------------------
      // ----------------------------------------------

      EXTENDED2
        .filter( evaluation._1 )
        .project(SOURCE.getAttributes().map(_.e) : _*)
    }

    // ------------------------------------------------

    REGIONFILTERED
  }

}
