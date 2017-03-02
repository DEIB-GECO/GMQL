package it.polimi.genomics.scidb.operators.region

import it.polimi.genomics.scidb.operators.{GDS, GmqlMetaJoinOperator, GmqlRegionOperator}
import it.polimi.genomics.scidb.utility.ExprUtils
import it.polimi.genomics.scidbapi.SciArray
import it.polimi.genomics.scidbapi.aggregate.AGGR_MAX
import it.polimi.genomics.scidbapi.expression.{V, OP, A}
import it.polimi.genomics.scidbapi.schema.DataType._
import it.polimi.genomics.scidbapi.schema.Attribute
import it.polimi.genomics.scidbapi.script.SciScript


/**
  * The it.polimi.genomics.scidb.test.operator implements the difference between
  * two datasets
  *
  * @param metajoin metajoin it.polimi.genomics.scidb.test.operator
  * @param positive dataset 1 used as base
  * @param negative dataset 2 used to define the negative regions
  */
class GmqlDifferenceRD(metajoin : GmqlMetaJoinOperator,
                       positive : GmqlRegionOperator,
                       negative : GmqlRegionOperator)
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
    val POSITIVE = positive.compute(script)
    var NEGATIVE = negative.compute(script)

    val METAJOIN = metajoin.compute(script)

    // ------------------------------------------------
    // pure join --------------------------------------

    NEGATIVE = NEGATIVE
      .redimension(
        List(GDS.sid_D, GDS.enumeration_D, GDS.chr_D),
        List(GDS.left_D.toAttribute(false), GDS.right_D.toAttribute(false), GDS.strand_D.toAttribute(false)))

    NEGATIVE = NEGATIVE
      .cast(NEGATIVE.getDimensions().map(_.label("NEGATIVE")), NEGATIVE.getAttributes().map(_.label("NEGATIVE")))

    val PUREJOIN = POSITIVE
      .cross_join(METAJOIN, "POSITIVE", "METAJOIN")(
        (GDS.sid_D.alias("POSITIVE").e, GDS.sid_ANCHOR_D.alias("METAJOIN").e))
      .cross_join(NEGATIVE, "POSITIVE", "NEGATIVE")(
        (GDS.sid_EXPERIMENT_D.alias("POSITIVE").e, GDS.sid_D.label("NEGATIVE").alias("NEGATIVE").e),
        (GDS.chr_D.alias("POSITIVE").e, GDS.chr_D.label("NEGATIVE").alias("NEGATIVE").e))

    // ------------------------------------------------
    // intersection -----------------------------------

    val RESULT = PUREJOIN
      .apply(( A("intersects"), ExprUtils.intersection(GDS.left_D.e, GDS.right_D.e, GDS.left_A.label("NEGATIVE").e, GDS.right_A.label("NEGATIVE").e) ))
      .redimension(GDS.regions_dimensions_D, POSITIVE.getAttributes() ::: List(Attribute("has_intersections", BOOL)), false, AGGR_MAX(A("intersects"),A("has_intersections")))
      .filter( OP(A("has_intersections"), "=", V(false)) )
      .redimension(POSITIVE.getDimensions(), POSITIVE.getAttributes())

    // ------------------------------------------------

    RESULT
  }

}
