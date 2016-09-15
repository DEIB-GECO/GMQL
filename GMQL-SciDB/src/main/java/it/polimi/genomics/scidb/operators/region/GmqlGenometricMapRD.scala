package it.polimi.genomics.scidb.operators.region

import it.polimi.genomics.core.DataStructures.RegionAggregate.R2RAggregator
import it.polimi.genomics.scidb.evaluators.AggregateEvaluator
import it.polimi.genomics.scidb.operators.{GDS, GmqlMetaJoinOperator, GmqlRegionOperator}
import it.polimi.genomics.scidb.utility.DebugUtils
import it.polimi.genomics.scidbapi.SciArray
import it.polimi.genomics.scidbapi.aggregate.AGGR
import it.polimi.genomics.scidbapi.expression._
import it.polimi.genomics.scidbapi.schema.Attribute
import it.polimi.genomics.scidbapi.script.SciScript


/**
  * The operator implements the map as defined by GMQL
  *
  * @param metajoin metajoin source
  * @param reference reference dataset source
  * @param experiment experiment dataset source
  * @param aggregates aggregators to be computed
  */
class GmqlGenometricMapRD(metajoin : GmqlMetaJoinOperator,
                          reference : GmqlRegionOperator,
                          experiment : GmqlRegionOperator,
                          aggregates : List[_<:R2RAggregator])
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
    val REFERENCE = reference.compute(script)
    val EXPERIMENT = experiment.compute(script)

    val METAJOIN = metajoin.compute(script)

    // ------------------------------------------------
    // Pure Join --------------------------------------

    val INTERSECTED = intersect(METAJOIN, REFERENCE, EXPERIMENT)

    // ------------------------------------------------
    // ------------------------------------------------

    // ------------------------------------------------
    // Aggregate --------------------------------------

    val AGGREGATED = aggregate(INTERSECTED, REFERENCE, EXPERIMENT, aggregates)

    // ------------------------------------------------
    // ------------------------------------------------

    AGGREGATED
  }


  // ------------------------------------------------------------
  // -- INTERSECT -----------------------------------------------

  /**
    * Execute the cross product between the regions considering
    * just the compatibles pairs. Filter the result considering
    * the region intersection
    *
    * @param METAJOIN
    * @param REFERENCE
    * @param EXPERIMENT
    * @return
    */
  def intersect(METAJOIN : SciArray,
                REFERENCE : SciArray,
                EXPERIMENT : SciArray)
    : SciArray =
  {
    /**
      * This method prepares a region data array to be used as
      * genometric join operand
      *
      * @param array array to be prepared
      * @param label disambiguation label
      * @return prepared array
      */
    def prepare(array:SciArray, label:String) : SciArray =
    {
      array
        .redimension(
          List(GDS.sid_D, GDS.enumeration_D, GDS.chr_D),
          List(GDS.left_D.toAttribute(false), GDS.right_D.toAttribute(false), GDS.strand_D.toAttribute(false)) ::: array.getAttributes())
        .cast(
          List(GDS.sid_D, GDS.enumeration_D, GDS.chr_D).map(d => d.label(label)),
          (List(GDS.left_A, GDS.right_A, GDS.strand_D.toAttribute()) ::: array.getAttributes()).map(a => a.label(label)))
    }

    // ------------------------------------------------
    // ------------------------------------------------

    val REFERENCE_prepared = prepare(REFERENCE, "ANCHOR")                                                               // prepare the reference dataset
    val EXPERIMENT_prepared = prepare(EXPERIMENT, "EXPERIMENT")                                                         // prepare the experiment dataset

    // ------------------------------------------------

    val left_REF    = GDS.left_D.label("ANCHOR").toAttribute().e
    val right_REF   = GDS.right_D.label("ANCHOR").toAttribute().e
    val strand_REF  = GDS.strand_D.label("ANCHOR").toAttribute().e
    val left_EXP    = GDS.left_D.label("EXPERIMENT").toAttribute().e
    val right_EXP   = GDS.right_D.label("EXPERIMENT").toAttribute().e
    val strand_EXP  = GDS.strand_D.label("EXPERIMENT").toAttribute().e

    // ------------------------------------------------

    val JOINED =
      if (false)
        REFERENCE_prepared
          .cross_join(EXPERIMENT_prepared, "ANCHOR", "EXPERIMENT")(
            (GDS.chr_D.label("ANCHOR").alias("ANCHOR").e, GDS.chr_D.label("EXPERIMENT").alias("EXPERIMENT").e))
          .apply((GDS.sid_RESULT_A.e, OP(OP(GDS.sid_ANCHOR_D.e, "*", V(10000)), "+", GDS.sid_EXPERIMENT_D.e)))
      else
        METAJOIN
          .cross_join(REFERENCE_prepared, "METAJOIN", "ANCHOR")(
            (GDS.sid_ANCHOR_D.alias("METAJOIN").e, GDS.sid_ANCHOR_D.alias("ANCHOR").e))
          .cross_join(EXPERIMENT_prepared, "ANCHOR", "EXPERIMENT")(
            (GDS.sid_EXPERIMENT_D.alias("ANCHOR").e, GDS.sid_EXPERIMENT_D.alias("EXPERIMENT").e),
            (GDS.chr_D.label("ANCHOR").alias("ANCHOR").e, GDS.chr_D.label("EXPERIMENT").alias("EXPERIMENT").e))

    val INTERSECTED = JOINED
      .filter(
        AND(
          OR( OP(strand_REF, "=", strand_EXP),                                                                          // filter only the regions with a compatible strands
            OR( OP(strand_REF, "=", V(1)), OP(strand_EXP, "=", V(1)))),
          OR(                                                                                                           // filter on intersection
            AND( OP(left_EXP, ">=", left_REF), OP(left_EXP, "<=", right_REF) ),
            AND( OP(left_EXP, "<=", left_REF), OP(left_REF, "<=", right_EXP) )
          )
        )
      )

    // ------------------------------------------------

    INTERSECTED
  }


  // ------------------------------------------------------------
  // -- AGGREGATE -----------------------------------------------

  /**
    * Applies the aggregation on the intersected input and
    * cast the result into the final form
    *
    * @param INTERSECTED
    * @param REFERENCE
    * @param EXPERIMENT
    * @param aggregates
    * @return
    */
  def aggregate(INTERSECTED : SciArray,
                REFERENCE : SciArray,
                EXPERIMENT : SciArray,
                aggregates : List[_<:R2RAggregator])
    : SciArray =
  {
    if(aggregates.isEmpty)
    {
      val RESULT = INTERSECTED
        .redimension(
          List(
            GDS.sid_D.label("RESULT"),
            GDS.chr_D.label("ANCHOR"),
            GDS.left_D.label("ANCHOR"),
            GDS.right_D.label("ANCHOR"),
            GDS.strand_D.label("ANCHOR"),
            GDS.enumeration_D.label("ANCHOR")
          ),
          REFERENCE.getAttributes().map(_.label("ANCHOR"))
        )
        .cast(GDS.regions_dimensions_D, REFERENCE.getAttributes())

      return RESULT
    }

    // ------------------------------------------------
    // ------------------------------------------------

    val operations = aggregates.map(item =>
      AggregateEvaluator(
        item.function_identifier,
        EXPERIMENT.getAttributes()(item.input_index).label("EXPERIMENT").e,
        (if(item.output_name.isDefined) A(item.output_name.get)
        else A(item.function_identifier+"_"+EXPERIMENT.getAttributes()(item.input_index).label("EXPERIMENT").name))
      )
    )

    val aggregators = operations.map(_._1).reduce((l1,l2) => l1:::l2)
    val aggregatorsAtrributes = aggregators.map(item => {
      val (_, datatype, name) = item.eval((INTERSECTED.getDimensions(), INTERSECTED.getAttributes()))
      Attribute(name, datatype)
    })

    val expressions = operations.map(e => if(e._2.isDefined) List(e._2.get) else List()).reduce((l1,l2) => l1:::l2)
    val expressionsAttributes = expressions.map(item => {
      val (_, datatype) = item._2.eval((INTERSECTED.getDimensions(), INTERSECTED.getAttributes():::aggregatorsAtrributes))
      Attribute(item._1.attribute, datatype)
    })

    val requiredAttributes = aggregates.map(item =>
      (if(item.output_name.isDefined) A(item.output_name.get)
       else A(item.function_identifier+"_"+EXPERIMENT.getAttributes()(item.input_index).label("EXPERIMENT").name))
    )
    val resultAttributes = (aggregatorsAtrributes ::: expressionsAttributes)
      .filter(attr => requiredAttributes.contains(attr.e))

    // ------------------------------------------------
    // ------------------------------------------------


    // ------------------------------------------------
    // ------------------------------------------------

    val AGGREGATED = INTERSECTED
      .redimension(
        List(
          GDS.sid_D.label("RESULT"),
          GDS.chr_D.label("ANCHOR"),
          GDS.left_D.label("ANCHOR"),
          GDS.right_D.label("ANCHOR"),
          GDS.strand_D.label("ANCHOR"),
          GDS.enumeration_D.label("ANCHOR")
        ),
        REFERENCE.getAttributes().map(_.label("ANCHOR")) ::: aggregatorsAtrributes,
        false,
        aggregators:_*
      )

    val APPLYED = if(expressions.isEmpty) AGGREGATED else AGGREGATED
      .apply(expressions:_*)

    val RESULT = APPLYED
      .cast(GDS.regions_dimensions_D, REFERENCE.getAttributes() ::: aggregatorsAtrributes ::: expressionsAttributes)
      .project((REFERENCE.getAttributes() ::: resultAttributes).map(_.e):_*)

    // ------------------------------------------------
    // ------------------------------------------------

    RESULT
  }

}
