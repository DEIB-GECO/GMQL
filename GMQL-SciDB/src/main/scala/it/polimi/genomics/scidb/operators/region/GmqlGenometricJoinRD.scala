package it.polimi.genomics.scidb.operators.region

import it.polimi.genomics.core.DataStructures.JoinParametersRD._
import it.polimi.genomics.core.DataStructures.JoinParametersRD.RegionBuilder.RegionBuilder
import it.polimi.genomics.scidb.exception.InternalErrorGmqlSciException
import it.polimi.genomics.scidb.operators.{GDS, GmqlMetaJoinOperator, GmqlRegionOperator}
import it.polimi.genomics.scidb.utility.JoinPredSteps
import it.polimi.genomics.scidbapi.SciArray
import it.polimi.genomics.scidbapi.expression._
import it.polimi.genomics.scidbapi.schema.DataType._
import it.polimi.genomics.scidbapi.schema.{Attribute, Dimension}
import it.polimi.genomics.scidbapi.script.{SciScript}


/**
  * Implements the genometric join operation as defined
  * by the official report
  *
  * @param metajoin metajoin source
  * @param anchor anchor dataset source
  * @param experiment experiment dataset source
  * @param predicates predicates to be applied
  * @param builder builing rule
  */
class GmqlGenometricJoinRD(metajoin : GmqlMetaJoinOperator,
                           anchor : GmqlRegionOperator,
                           experiment : GmqlRegionOperator,
                           predicates : List[JoinPredSteps],
                           builder : RegionBuilder)
  extends GmqlRegionOperator
{

  val enum_ANCHOR_D = GDS.enumeration_D.label("ANCHOR")
  val enum_EXPERIMENT_D = GDS.enumeration_D.label("EXPERIMENT")

  // ------------------------------------------------------------
  // ------------------------------------------------------------

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
    // Pure Join --------------------------------------

    val PUREJOIN = join(METAJOIN, ANCHOR, EXPERIMENT)

    // ------------------------------------------------
    // ------------------------------------------------


    // ------------------------------------------------
    // Predicates application -------------------------

    val FILTEREDJOIN = filter(PUREJOIN, predicates)

    // ------------------------------------------------
    // ------------------------------------------------


    // ------------------------------------------------
    // Rebuild array ----------------------------------

    val REBUILDED = rebuild(FILTEREDJOIN, ANCHOR, EXPERIMENT, builder)

    // ------------------------------------------------
    // ------------------------------------------------

    REBUILDED
  }


  // ------------------------------------------------------------
  // -- JOIN ----------------------------------------------------

  /**
    * This methods computes the cross product between the two
    * datasets returning all the possible pairs
    *
    * @param METAJOIN metajoin result
    * @param ANCHOR anchor dataset
    * @param EXPERIMENT experiment dataset
    * @return an array containing the cross product
    */
  def join(METAJOIN : SciArray,
           ANCHOR : SciArray,
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
          List(GDS.left_D.toAttribute(false), GDS.right_D.toAttribute(false), GDS.strand_D.toAttribute(false)))
        .cast(
          List(GDS.sid_D, GDS.enumeration_D, GDS.chr_D).map(d => d.label(label)),
          List(GDS.left_A, GDS.right_A, GDS.strand_D.toAttribute()).map(a => a.label(label)))
    }

    // ------------------------------------------------
    // ------------------------------------------------

    val ANCHOR_prepared = prepare(ANCHOR, "ANCHOR")                                                                     // prepare the anchor dataset
    val EXPERIMENT_prepared = prepare(EXPERIMENT, "EXPERIMENT")                                                         // prepare the experiment dataset

    // ------------------------------------------------

    val left_ANC    = GDS.left_D.label("ANCHOR").toAttribute().e
    val right_ANC   = GDS.right_D.label("ANCHOR").toAttribute().e
    val strand_ANC  = GDS.strand_D.label("ANCHOR").toAttribute().e
    val left_EXP    = GDS.left_D.label("EXPERIMENT").toAttribute().e
    val right_EXP   = GDS.right_D.label("EXPERIMENT").toAttribute().e
    val strand_EXP  = GDS.strand_D.label("EXPERIMENT").toAttribute().e

    // ------------------------------------------------

    val PUREJOIN = METAJOIN
      .cross_join(ANCHOR_prepared, "METAJOIN", "ANCHOR")(                                                               // join metajoin sid with the correct anchor
        (GDS.sid_ANCHOR_D.alias("METAJOIN").e, GDS.sid_ANCHOR_D.alias("ANCHOR").e))
      .cross_join(EXPERIMENT_prepared, "ANCHOR", "EXPERIMENT")(                                                         // join metajoin sid with the correct experiment
        (GDS.sid_EXPERIMENT_D.alias("ANCHOR").e, GDS.sid_EXPERIMENT_D.alias("EXPERIMENT").e),
        (GDS.chr_D.label("ANCHOR").alias("ANCHOR").e, GDS.chr_D.label("EXPERIMENT").alias("EXPERIMENT").e))
      .filter(                                                                                                          // filter only the regions with a compatible strands
        OR( OP(strand_ANC, "=", strand_EXP),
          OR( OP(strand_ANC, "=", V(1)), OP(strand_EXP, "=", V(1))))
      )
      .apply(
        (
          A("distance"),                                                                                                // computes the genometric distance between regions
          IF( OP(FUN("abs", OP(left_EXP,"-",right_ANC)), "<", FUN("abs", OP(left_ANC,"-",right_EXP))),
            OP(left_EXP,"-",right_ANC),
            OP(left_ANC,"-",right_EXP))
        ),(
          A("upstream"),                                                                                                // computes the upstream flag
          IF(
            OR(
              AND(OR( OP(strand_ANC, "=", V(2)), OP(strand_EXP, "=", V(2))),
                OP(left_EXP, "<=", right_ANC)),
              AND(OR( OP(strand_ANC, "=", V(0)), OP(strand_EXP, "=", V(0))),
                OP(right_EXP, ">=", left_ANC))
            ), V(1), V(0)
          )
        ),(
          A("downstream"),                                                                                              // computes the downstream flag
          IF(
            OR(
              AND(OR( OP(strand_ANC, "=", V(2)), OP(strand_EXP, "=", V(2))),
                OP(right_EXP, ">=", left_ANC)),
              AND(OR( OP(strand_ANC, "=", V(0)), OP(strand_EXP, "=", V(0))),
                OP(left_EXP, "<=", right_ANC))
            ), V(1), V(0)
          )
        )
      )
      .project(A("distance"), A("upstream"), A("downstream"))                                                           // keep only the computed distance in the result

    // ------------------------------------------------

    PUREJOIN
  }


  // ------------------------------------------------------------
  // -- FILTER --------------------------------------------------

  /**
    * This method filters the joined array using a list of filters
    *
    * @param ARRAY source array
    * @param predicates filter predicates to be applied
    * @return the filtered array
    */
  def filter(ARRAY : SciArray,
             predicates : List[JoinPredSteps])
    : SciArray =
  {
    /**
      * This method return the filtered result for a single
      * predicates plan
      *
      * @param ARRAY array to be filtered
      * @param predicate predicates plan
      * @return
      */
    def singlefilter(ARRAY : SciArray,
                     predicate : JoinPredSteps)
      : SciArray =
    {
      val upstream_D = Dimension("upstream", 0, Some(1), 100, 0)
      val downstream_D = Dimension("downstream", 0, Some(1), 100, 0)

      val distance_A = Attribute("distance", INT64)
      val upstream_A = upstream_D.toAttribute(false)
      val downstream_A = downstream_D.toAttribute(false)

      // ----------------------------------------------

      val ARRAY_step1 = if( predicate.first.isEmpty ) ARRAY else                                                        // if required applies the first step
      {                                                                                                                 // of filters
        val filter = predicate.first
          .map(_ match {
            case DistLess(limit) => OP(distance_A.e, "<=", V(limit))
            case DistGreater(limit) => OP(distance_A.e, ">=", V(limit))
            case Upstream() => OP(upstream_A.e, "=", V(1))
            case DownStream() => OP(downstream_A.e, "=", V(1))
            case pred => throw new InternalErrorGmqlSciException("Predicate '" + pred + "' not valid in the first step")
          })
          .reduceRight((p1, p2) => OP(p1, "and", p2))

        ARRAY.filter(filter)
      }

      // ----------------------------------------------

      val distance_rank_A = Attribute(distance_A.name+"_rank", DOUBLE)

      val ARRAY_step2 = if( predicate.second.isEmpty ) ARRAY_step1 else                                                 // if required applies the second step
      {                                                                                                                 // of filters
        var limit = predicate.second.head.asInstanceOf[MinDistance].number

        ARRAY_step1
          .redimension(
            ARRAY.getDimensions() ::: List(upstream_D, downstream_D),
            List(distance_A)
          )
          .rank(distance_A.e)(GDS.sid_ANCHOR_D.e, GDS.sid_EXPERIMENT_D.e)
          .filter( OP(distance_rank_A.e, "<=", V(limit)) )
          .redimension(
            ARRAY.getDimensions(),
            List(distance_A, upstream_A, downstream_A)
          )
          .cast(
            ARRAY.getDimensions(),
            ARRAY.getAttributes()
          )
      }

      // ----------------------------------------------

      val ARRAY_step3 = if( predicate.third.isEmpty ) ARRAY_step2 else                                                  // if required applies the third step
      {                                                                                                                 // of filters
        val filter = predicate.third
          .map(_ match {
            case DistGreater(limit) => OP(distance_A.e, ">=", V(limit))
            case Upstream() => OP(upstream_A.e, "=", V(1))
            case DownStream() => OP(downstream_A.e, "=", V(1))
            case pred => throw new InternalErrorGmqlSciException("Predicate '" + pred + "' not valid in the first step")
          })
          .reduceRight((p1, p2) => OP(p1, "and", p2))

        ARRAY_step2.filter(filter)
      }

      // ----------------------------------------------

      ARRAY_step3
    }

    // ------------------------------------------------
    // Application ------------------------------------

    val FILTERED = predicates
      .map(item => singlefilter(ARRAY, item))                                                                           // perform all the required filters
      .reduce((j1,j2) => j1 merge j2)                                                                                   // merge the result into a unique array

    // ------------------------------------------------

    FILTERED
  }


  // ------------------------------------------------------------
  // -- REBUILD -------------------------------------------------

  /**
    * This method rebuild the result according to the required
    * configuration
    *
    * @param RESULT filtered array containing the regions list
    * @param ANCHOR the anchor source
    * @param EXPERIMENT the experiment source
    * @param rule the building rule
    * @return the builded array
    */
  def rebuild(RESULT : SciArray,
              ANCHOR : SciArray,
              EXPERIMENT : SciArray,
              rule : RegionBuilder)
    : SciArray =
  {

    // ------------------------------------------------
    // Pepare -----------------------------------------

    /**
      * This method prepares a region data array to be rebuilt
      *
      * @param array array to be prepared
      * @param label disambiguation label
      * @return prepared array
      */
    def prepare(array:SciArray, label:String) : SciArray =
    {
      val redimensioned = array
        .redimension(
          List(GDS.sid_D, GDS.enumeration_D),
          GDS.coordinates_dimensions_A ::: array.getAttributes()
        )
      val casted = redimensioned.cast(
        redimensioned.getDimensions().map(dim => dim.label(label)),
        redimensioned.getAttributes().map(attr => attr.label(label))
      )
      casted
    }

    // ------------------------------------------------

    val aL = "ANC"
    val eL = "EXP"

    val RESULT_prepared = RESULT
      .apply((GDS.sid_D.toAttribute(false).e, OP(OP(GDS.sid_ANCHOR_D.e, "*", V(10000)), "+", GDS.sid_EXPERIMENT_D.e)))
      .redimension(
        List(
          GDS.sid_D,
          GDS.sid_ANCHOR_D,
          GDS.sid_EXPERIMENT_D,
          enum_ANCHOR_D,
          enum_EXPERIMENT_D
        ),
        List(Attribute("distance", INT64))
      )

    val ANCHOR_prepared = prepare(ANCHOR, aL)
    val EXPERIMENT_prepared = prepare(EXPERIMENT, eL)

    // ------------------------------------------------
    // Join -------------------------------------------

    val JOINED = RESULT_prepared
      .cross_join(ANCHOR_prepared, "RESULT", "ANCHOR")(
        (GDS.sid_ANCHOR_D.alias("RESULT").e, GDS.sid_D.label(aL).alias("ANCHOR").e),
        (enum_ANCHOR_D.alias("RESULT").e, GDS.enumeration_D.label(aL).alias("ANCHOR").e)
      )
      .cross_join(EXPERIMENT_prepared, "RESULT", "EXPERIMENT")(
        (GDS.sid_EXPERIMENT_D.alias("RESULT").e, GDS.sid_D.label(eL).alias("EXPERIMENT").e),
        (enum_EXPERIMENT_D.alias("RESULT").e, GDS.enumeration_D.label(eL).alias("EXPERIMENT").e)
      )

    // ------------------------------------------------
    // Pre-build --------------------------------------

    /**
      * This function prepare the attributes required for the
      * intersection building
      *
      * @param ARRAY source array
      * @return
      */
    def prebuildIntersection(ARRAY:SciArray) : (SciArray, String) =
    {
      val PREBUILDED = ARRAY
        .apply(
          (GDS.left_A.label("INT").e, IF(OP(GDS.left_A.label(aL).e, ">", GDS.left_A.label(eL).e), GDS.left_A.label(aL).e, GDS.left_A.label(eL).e)),
          (GDS.right_A.label("INT").e, IF(OP(GDS.right_A.label(aL).e, "<", GDS.right_A.label(eL).e), GDS.right_A.label(aL).e, GDS.right_A.label(eL).e)),
          (GDS.strand_A.label("INT").e, IF(OP(GDS.strand_A.label(aL).e, "<>", V(1)), GDS.strand_A.label(aL).e, GDS.strand_A.label(eL).e))
        )
        .filter(
          OP(GDS.left_A.label("INT").e, "<", GDS.right_A.label("INT").e)
        )
      (PREBUILDED, "INT")
    }

    /**
      * This function prepare the attributes required for the
      * concatenation building
      *
      * @param ARRAY source array
      * @return
      */
    def prebuildConcatenation(ARRAY:SciArray) : (SciArray, String) =
    {
      val PREBUILDED = ARRAY
        .apply(
          (GDS.left_A.label("CAT").e, IF(OP(GDS.left_A.label(aL).e, "<", GDS.left_A.label(eL).e), GDS.left_A.label(aL).e, GDS.left_A.label(eL).e)),
          (GDS.right_A.label("CAT").e, IF(OP(GDS.right_A.label(aL).e, ">", GDS.right_A.label(eL).e), GDS.right_A.label(aL).e, GDS.right_A.label(eL).e)),
          (GDS.strand_A.label("CAT").e, IF(OP(GDS.strand_A.label(aL).e, "<>", V(1)), GDS.strand_A.label(aL).e, GDS.strand_A.label(eL).e))
        )
      (PREBUILDED, "CAT")
    }

    // ------------------------------------------------

    val PREBUILDED = rule match
    {
      case RegionBuilder.LEFT => (JOINED, aL)
      case RegionBuilder.RIGHT => (JOINED, eL)
      case RegionBuilder.INTERSECTION => prebuildIntersection(JOINED)
      case RegionBuilder.CONTIG => prebuildConcatenation(JOINED)
    }

    // ------------------------------------------------
    // Build ------------------------------------------

    var anchorAttributes = ANCHOR.getAttributes().map(attr => attr.label(aL))
    var experimentAttributes = EXPERIMENT.getAttributes().map(attr => attr.label(eL))

    val FINAL = PREBUILDED._1
      .unpack(GDS.enumeration_D.e, GDS.enumeration_D.chunk_length)
      .redimension(
        List(
          GDS.sid_D,
          GDS.chr_D.label(aL),
          GDS.left_D.label(PREBUILDED._2),
          GDS.right_D.label(PREBUILDED._2),
          GDS.strand_D.label(PREBUILDED._2),
          GDS.enumeration_D
        ),
        anchorAttributes ::: experimentAttributes
      )
      .cast(GDS.regions_dimensions_D, anchorAttributes ::: experimentAttributes)

    // ------------------------------------------------

    FINAL
  }

}
